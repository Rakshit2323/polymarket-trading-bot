package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"poly-gocopy/internal/dotenv"
)

type config struct {
	sshServer                string
	sshPassword              string
	sshKeyPath               string
	sshPort                  string
	sshUseSudo               bool
	remoteDir                string
	serviceName              string
	arbitrageService         string
	arbitrageEqualService    string
	arbitrageWeightedService string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	cfg := loadConfig()

	if len(os.Args) > 1 {
		runCommand(cfg, os.Args[1])
		return
	}

	runInteractive(cfg)
}

func loadConfig() config {
	sshUseSudo := false
	if v := strings.TrimSpace(os.Getenv("SSH_USE_SUDO")); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "on":
			sshUseSudo = true
		}
	}

	return config{
		sshServer:                os.Getenv("SSH_SERVER"),
		sshPassword:              os.Getenv("SSH_PASSWORD"),
		sshKeyPath:               os.Getenv("SSH_KEY_PATH"),
		sshPort:                  firstNonEmpty(os.Getenv("SSH_PORT"), "22"),
		sshUseSudo:               sshUseSudo,
		remoteDir:                firstNonEmpty(os.Getenv("DEPLOY_REMOTE_DIR"), "/opt/poly-gocopy"),
		serviceName:              os.Getenv("DEPLOY_SERVICE_NAME"),
		arbitrageService:         firstNonEmpty(os.Getenv("ARBITRAGE_SERVICE"), "poly-gocopy-arbitrage"),
		arbitrageEqualService:    firstNonEmpty(os.Getenv("ARBITRAGE_EQUAL_SERVICE"), "poly-gocopy-arbitrage-equal"),
		arbitrageWeightedService: firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_SERVICE"), "poly-gocopy-arbitrage-weighted"),
	}
}

func runInteractive(cfg config) {
	reader := bufio.NewReader(os.Stdin)

	for {
		printMenu()
		fmt.Print("\nSelect option: ")

		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("\nGoodbye!")
				return
			}
			log.Printf("Error reading input: %v", err)
			continue
		}

		choice := strings.TrimSpace(input)
		if choice == "" {
			continue
		}

		switch choice {
		case "1":
			deployService(cfg, "arbitrage")
		case "2":
			deployService(cfg, "arbitrage-equal")
		case "3":
			deployService(cfg, "arbitrage-weighted")
		case "4":
			deployBoth(cfg)
		case "5":
			pushSource(cfg)
		case "6":
			restartServices(cfg)
		case "7":
			reloadServices(cfg)
		case "8":
			stopServices(cfg)
		case "9":
			showStatus(cfg)
		case "10":
			showLogs(cfg, 80)
		case "11":
			followLogs(cfg)
		case "12":
			removeServiceInteractive(cfg, reader)
		case "13":
			removeServices(cfg)
		case "0", "q", "quit", "exit":
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Printf("Unknown option: %s\n", choice)
		}

		fmt.Println()
	}
}

func printMenu() {
	fmt.Println()
	fmt.Println("=== poly-gocopy Deploy CLI ===")
	fmt.Println()
	fmt.Println("  1) Deploy arbitrage")
	fmt.Println("  2) Deploy arbitrage-equal")
	fmt.Println("  3) Deploy arbitrage-weighted")
	fmt.Println("  4) Deploy all")
	fmt.Println("  5) Push source (rsync)")
	fmt.Println("  6) Restart services")
	fmt.Println("  7) Reload services (hot-reload)")
	fmt.Println("  8) Stop services")
	fmt.Println("  9) Show status")
	fmt.Println(" 10) Show logs")
	fmt.Println(" 11) Follow logs")
	fmt.Println(" 12) Remove service")
	fmt.Println(" 13) Remove services (uninstall)")
	fmt.Println("  0) Exit")
}

func runCommand(cfg config, cmd string) {
	switch cmd {
	case "deploy-arbitrage":
		deployService(cfg, "arbitrage")
	case "deploy-arbitrage-equal", "deploy-equal":
		deployService(cfg, "arbitrage-equal")
	case "deploy-arbitrage-weighted", "deploy-weighted":
		deployService(cfg, "arbitrage-weighted")
	case "deploy-both", "deploy-all", "deploy":
		deployBoth(cfg)
	case "push-source", "push":
		pushSource(cfg)
	case "restart":
		restartServices(cfg)
	case "reload":
		reloadServices(cfg)
	case "stop":
		stopServices(cfg)
	case "status":
		showStatus(cfg)
	case "logs":
		showLogs(cfg, 80)
	case "follow":
		followLogs(cfg)
	case "remove-arbitrage":
		removeService(cfg, "arbitrage")
	case "remove-arbitrage-equal", "remove-equal":
		removeService(cfg, "arbitrage-equal")
	case "remove-arbitrage-weighted", "remove-weighted":
		removeService(cfg, "arbitrage-weighted")
	case "remove-all", "remove", "uninstall":
		removeServices(cfg)
	case "help":
		printHelp()
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printHelp()
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println(`Usage: deploy [command]

Commands:
  deploy-arbitrage          Deploy arbitrage service
  deploy-arbitrage-equal    Deploy arbitrage-equal service
  deploy-arbitrage-weighted Deploy arbitrage-weighted service
  deploy-all                Deploy all services
  deploy-both               Alias for deploy-all
  push-source               Push source tree via rsync
  restart                   Restart services
  reload                    Reload services (hot-reload)
  stop                      Stop services
  status                    Show service status
  logs                      Show recent logs
  follow                    Follow logs in real-time
  remove-arbitrage          Remove/uninstall arbitrage service
  remove-arbitrage-equal    Remove/uninstall arbitrage-equal service
  remove-arbitrage-weighted Remove/uninstall arbitrage-weighted service
  remove-all                Remove/uninstall all services
  remove                    Alias for remove-all
  help                      Show this help

If no command is given, runs in interactive mode.

Configuration via .env:
  SSH_SERVER        user@host (required)
  SSH_PASSWORD      Password for SSH (or use SSH_KEY_PATH)
  SSH_KEY_PATH      Path to SSH private key
  SSH_PORT          SSH port (default: 22)
  SSH_USE_SUDO      Use sudo for remote commands (1/true/yes)
  DEPLOY_REMOTE_DIR Remote directory (default: /opt/poly-gocopy)
  ARBITRAGE_SERVICE           Service name for arbitrage
  ARBITRAGE_EQUAL_SERVICE     Service name for arbitrage-equal
  ARBITRAGE_WEIGHTED_SERVICE  Service name for arbitrage-weighted`)
}

func getSSHClient(cfg config) (*ssh.Client, error) {
	if cfg.sshServer == "" {
		return nil, fmt.Errorf("SSH_SERVER not configured in .env")
	}

	var authMethods []ssh.AuthMethod

	// Try key auth first
	if cfg.sshKeyPath != "" {
		keyPath := cfg.sshKeyPath
		if strings.HasPrefix(keyPath, "~") {
			home, _ := os.UserHomeDir()
			keyPath = filepath.Join(home, keyPath[1:])
		}

		key, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("read SSH key %s: %w", keyPath, err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("parse SSH key: %w", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	} else if cfg.sshPassword != "" {
		authMethods = append(authMethods, ssh.Password(cfg.sshPassword))
	} else {
		return nil, fmt.Errorf("SSH_PASSWORD or SSH_KEY_PATH required in .env")
	}

	// Parse user@host
	parts := strings.SplitN(cfg.sshServer, "@", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("SSH_SERVER must be user@host format, got: %s", cfg.sshServer)
	}
	user := parts[0]
	host := parts[1]

	sshConfig := &ssh.ClientConfig{
		User:            user,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	addr := net.JoinHostPort(host, cfg.sshPort)
	client, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("SSH connect to %s: %w", addr, err)
	}

	return client, nil
}

func runRemoteCommand(cfg config, cmd string) (string, error) {
	client, err := getSSHClient(cfg)
	if err != nil {
		return "", err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("create SSH session: %w", err)
	}
	defer session.Close()

	if cfg.sshUseSudo {
		cmd = "sudo -n " + cmd
	}

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	err = session.Run(cmd)
	output := stdout.String()
	if stderr.Len() > 0 {
		output += stderr.String()
	}

	if err != nil {
		return output, fmt.Errorf("remote command failed: %w\nOutput: %s", err, output)
	}

	return output, nil
}

func runRemoteCommandStreaming(cfg config, cmd string) error {
	client, err := getSSHClient(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("create SSH session: %w", err)
	}
	defer session.Close()

	if cfg.sshUseSudo {
		cmd = "sudo -n " + cmd
	}

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr

	return session.Run(cmd)
}

func getRemoteArch(cfg config) (string, error) {
	output, err := runRemoteCommand(cfg, "uname -m")
	if err != nil {
		return "", err
	}

	arch := strings.TrimSpace(output)
	switch arch {
	case "x86_64":
		return "amd64", nil
	case "aarch64", "arm64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("unsupported remote architecture: %s", arch)
	}
}

func buildBinary(cmdName, goarch string) (string, error) {
	outDir := filepath.Join("out", "deploy", cmdName)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return "", fmt.Errorf("create output dir: %w", err)
	}

	binaryPath := filepath.Join(outDir, cmdName)

	fmt.Printf("Building %s for linux/%s...\n", cmdName, goarch)

	cmd := exec.Command("go", "build", "-trimpath", "-ldflags=-s -w", "-o", binaryPath, "./cmd/"+cmdName)
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+goarch,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build failed: %w", err)
	}

	return binaryPath, nil
}

func uploadFile(cfg config, localPath, remotePath string) error {
	client, err := getSSHClient(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	// Read local file
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("read local file: %w", err)
	}

	// Create SCP session
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("create SSH session: %w", err)
	}
	defer session.Close()

	// Use cat for file upload (simpler than full SCP protocol)
	remoteCmd := fmt.Sprintf("cat > %s", remotePath)
	if cfg.sshUseSudo {
		// Upload to temp first, then move
		tempPath := "/tmp/" + filepath.Base(remotePath) + ".upload"
		remoteCmd = fmt.Sprintf("cat > %s", tempPath)
	}

	stdin, err := session.StdinPipe()
	if err != nil {
		return fmt.Errorf("get stdin pipe: %w", err)
	}

	if err := session.Start(remoteCmd); err != nil {
		return fmt.Errorf("start remote command: %w", err)
	}

	if _, err := stdin.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}
	stdin.Close()

	if err := session.Wait(); err != nil {
		return fmt.Errorf("remote command failed: %w", err)
	}

	// If using sudo, move from temp to final location
	if cfg.sshUseSudo {
		tempPath := "/tmp/" + filepath.Base(remotePath) + ".upload"
		moveCmd := fmt.Sprintf("sudo -n mv %s %s", tempPath, remotePath)
		if _, err := runRemoteCommand(cfg, moveCmd); err != nil {
			return fmt.Errorf("move file with sudo: %w", err)
		}
	}

	return nil
}

func deployService(cfg config, cmdName string) {
	fmt.Printf("\n=== Deploying %s ===\n\n", cmdName)

	// Get remote architecture
	goarch, err := getRemoteArch(cfg)
	if err != nil {
		fmt.Printf("Error getting remote arch: %v\n", err)
		return
	}
	fmt.Printf("Remote architecture: %s\n", goarch)

	// Build binary
	binaryPath, err := buildBinary(cmdName, goarch)
	if err != nil {
		fmt.Printf("Error building: %v\n", err)
		return
	}
	fmt.Printf("Built: %s\n", binaryPath)

	// Also build the balance utility and deploy it alongside the service binary.
	balancePath, balanceErr := buildBinary("balance", goarch)
	if balanceErr != nil {
		fmt.Printf("[warn] failed to build balance utility: %v\n", balanceErr)
	} else {
		fmt.Printf("Built (utility): %s\n", balancePath)
	}

	// Generate systemd unit file
	serviceName := serviceNameForCommand(cfg, cmdName)
	unitContent := generateSystemdUnit(cmdName, cfg.remoteDir, serviceName)

	// Ensure remote directories exist
	fmt.Println("Creating remote directories...")
	mkdirCmd := fmt.Sprintf("mkdir -p %s/bin %s/out", cfg.remoteDir, cfg.remoteDir)
	if _, err := runRemoteCommand(cfg, mkdirCmd); err != nil {
		fmt.Printf("Error creating directories: %v\n", err)
		return
	}

	// Upload binary
	remoteBinPath := fmt.Sprintf("%s/bin/%s", cfg.remoteDir, cmdName)
	tempBinPath := fmt.Sprintf("/tmp/%s.new", cmdName)

	fmt.Printf("Uploading binary to %s...\n", remoteBinPath)
	if err := uploadFile(cfg, binaryPath, tempBinPath); err != nil {
		fmt.Printf("Error uploading binary: %v\n", err)
		return
	}

	hasBalance := false
	remoteBalancePath := fmt.Sprintf("%s/bin/balance", cfg.remoteDir)
	tempBalancePath := "/tmp/balance.new"
	if balancePath != "" && balanceErr == nil {
		fmt.Printf("Uploading balance utility to %s...\n", remoteBalancePath)
		if err := uploadFile(cfg, balancePath, tempBalancePath); err != nil {
			fmt.Printf("[warn] failed to upload balance utility: %v\n", err)
		} else {
			hasBalance = true
		}
	}

	// Upload systemd unit file
	remoteUnitPath := fmt.Sprintf("/etc/systemd/system/%s.service", serviceName)
	tempUnitPath := fmt.Sprintf("/tmp/%s.service.new", serviceName)

	fmt.Printf("Uploading systemd unit to %s...\n", remoteUnitPath)
	if err := uploadContent(cfg, []byte(unitContent), tempUnitPath); err != nil {
		fmt.Printf("Error uploading unit file: %v\n", err)
		return
	}

	// Install everything atomically
	fmt.Println("Installing files...")
	var installCmd string
	if cfg.sshUseSudo {
		installCmd = fmt.Sprintf("sudo -n chmod 700 %s && sudo -n mv -f %s %s", tempBinPath, tempBinPath, remoteBinPath)
		if hasBalance {
			installCmd += fmt.Sprintf(" && sudo -n chmod 700 %s && sudo -n mv -f %s %s", tempBalancePath, tempBalancePath, remoteBalancePath)
		}
		installCmd += fmt.Sprintf(
			" && sudo -n chmod 644 %s && sudo -n mv -f %s %s && "+
				"sudo -n systemctl daemon-reload && "+
				"sudo -n systemctl enable --now %s && "+
				"sudo -n systemctl restart %s",
			tempUnitPath, tempUnitPath, remoteUnitPath,
			serviceName,
			serviceName,
		)
	} else {
		installCmd = fmt.Sprintf("chmod 700 %s && mv -f %s %s", tempBinPath, tempBinPath, remoteBinPath)
		if hasBalance {
			installCmd += fmt.Sprintf(" && chmod 700 %s && mv -f %s %s", tempBalancePath, tempBalancePath, remoteBalancePath)
		}
		installCmd += fmt.Sprintf(
			" && chmod 644 %s && mv -f %s %s && "+
				"systemctl daemon-reload && "+
				"systemctl enable --now %s && "+
				"systemctl restart %s",
			tempUnitPath, tempUnitPath, remoteUnitPath,
			serviceName,
			serviceName,
		)
	}
	if output, err := runRemoteCommand(cfg, installCmd); err != nil {
		fmt.Printf("Error installing: %v\n%s\n", err, output)
		return
	}

	// Show status
	fmt.Println("\nService status:")
	statusCmd := fmt.Sprintf("systemctl status %s --no-pager || true", serviceName)
	if output, err := runRemoteCommand(cfg, statusCmd); err == nil {
		fmt.Println(output)
	}

	fmt.Printf("\n=== %s deployed successfully ===\n", cmdName)
}

func generateSystemdUnit(cmdName, remoteDir, serviceName string) string {
	var sb strings.Builder

	sb.WriteString("[Unit]\n")
	sb.WriteString(fmt.Sprintf("Description=poly-gocopy (%s)\n", cmdName))
	sb.WriteString("After=network-online.target\n")
	sb.WriteString("Wants=network-online.target\n")
	sb.WriteString("\n")

	sb.WriteString("[Service]\n")
	sb.WriteString("Type=simple\n")
	sb.WriteString(fmt.Sprintf("WorkingDirectory=%s\n", remoteDir))
	sb.WriteString(fmt.Sprintf("EnvironmentFile=%s/.env\n", remoteDir))
	sb.WriteString(fmt.Sprintf("ExecStart=%s/bin/%s\n", remoteDir, cmdName))

	if cmdName == "arbitrage" || cmdName == "arbitrage-equal" || cmdName == "arbitrage-weighted" {
		// arbitrage supports hot-reloading its event slugs file via SIGHUP
		sb.WriteString("ExecReload=/bin/kill -HUP $MAINPID\n")
	}

	sb.WriteString("Restart=on-failure\n")
	sb.WriteString("RestartSec=2\n")
	sb.WriteString("UMask=0077\n")
	sb.WriteString("NoNewPrivileges=true\n")
	sb.WriteString("PrivateTmp=true\n")
	sb.WriteString("ProtectHome=true\n")
	sb.WriteString("ProtectSystem=strict\n")
	sb.WriteString(fmt.Sprintf("ReadWritePaths=%s/out\n", remoteDir))
	sb.WriteString("\n")

	sb.WriteString("[Install]\n")
	sb.WriteString("WantedBy=multi-user.target\n")

	return sb.String()
}

func uploadContent(cfg config, content []byte, remotePath string) error {
	client, err := getSSHClient(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("create SSH session: %w", err)
	}
	defer session.Close()

	remoteCmd := fmt.Sprintf("cat > %s", remotePath)

	stdin, err := session.StdinPipe()
	if err != nil {
		return fmt.Errorf("get stdin pipe: %w", err)
	}

	if err := session.Start(remoteCmd); err != nil {
		return fmt.Errorf("start remote command: %w", err)
	}

	if _, err := stdin.Write(content); err != nil {
		return fmt.Errorf("write data: %w", err)
	}
	stdin.Close()

	if err := session.Wait(); err != nil {
		return fmt.Errorf("remote command failed: %w", err)
	}

	return nil
}

func deployBoth(cfg config) {
	deployService(cfg, "arbitrage")
	fmt.Println()
	deployService(cfg, "arbitrage-equal")
	fmt.Println()
	deployService(cfg, "arbitrage-weighted")
}

func pushSource(cfg config) {
	fmt.Print("\n=== Pushing source tree ===\n\n")

	if cfg.sshServer == "" {
		fmt.Println("Error: SSH_SERVER not configured")
		return
	}

	// Build SSH command for rsync
	sshCmd := "ssh -o StrictHostKeyChecking=accept-new"
	if cfg.sshPort != "22" {
		sshCmd += " -p " + cfg.sshPort
	}
	if cfg.sshKeyPath != "" {
		keyPath := cfg.sshKeyPath
		if strings.HasPrefix(keyPath, "~") {
			home, _ := os.UserHomeDir()
			keyPath = filepath.Join(home, keyPath[1:])
		}
		sshCmd += " -o BatchMode=yes"
		sshCmd += " -o PreferredAuthentications=publickey"
		sshCmd += " -o PasswordAuthentication=no"
		sshCmd += " -o PubkeyAuthentication=yes"
		sshCmd += " -o IdentitiesOnly=yes"
		sshCmd += " -i " + keyPath
	} else if cfg.sshPassword != "" {
		sshCmd += " -o PreferredAuthentications=password"
		sshCmd += " -o PubkeyAuthentication=no"
	}

	// Build rsync command
	rsyncArgs := []string{
		"-az",
		"--info=progress2",
		"--human-readable",
		"--exclude=.git/",
		"--exclude=out/",
		"--exclude=.tmp/",
		"--exclude=bin/",
		"--include=.env.example",
		"--exclude=.env",
		"--exclude=.env.*",
		"-e", sshCmd,
		"./",
		cfg.sshServer + ":" + cfg.remoteDir + "/",
	}

	fmt.Printf("Running: rsync %s\n\n", strings.Join(rsyncArgs, " "))

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = nil

	// For password auth, use SSH_ASKPASS with setsid (same as original bash scripts)
	if cfg.sshPassword != "" && cfg.sshKeyPath == "" {
		// Create a temporary askpass script
		askpassScript, err := createAskpassScript(cfg.sshPassword)
		if err != nil {
			fmt.Printf("Error creating askpass script: %v\n", err)
			return
		}
		defer os.Remove(askpassScript)

		// Use setsid to detach from terminal so SSH_ASKPASS works
		cmd = exec.Command("setsid", append([]string{"-w", "rsync"}, rsyncArgs...)...)
		cmd.Env = append(os.Environ(),
			"SSH_ASKPASS="+askpassScript,
			"SSH_ASKPASS_REQUIRE=force",
			"DISPLAY=dummy",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = nil
	}

	if err := cmd.Run(); err != nil {
		fmt.Printf("Error: rsync failed: %v\n", err)
		return
	}

	fmt.Println("\n=== Source pushed successfully ===")
}

func createAskpassScript(password string) (string, error) {
	// Create temp directory if needed
	outDir := filepath.Join("out", "deploy")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return "", err
	}

	askpassPath := filepath.Join(outDir, "ssh_askpass.sh")

	// Write the askpass script - it just prints the password
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' '%s'\n", strings.ReplaceAll(password, "'", "'\"'\"'"))
	if err := os.WriteFile(askpassPath, []byte(script), 0700); err != nil {
		return "", err
	}

	// Return absolute path
	absPath, err := filepath.Abs(askpassPath)
	if err != nil {
		return askpassPath, nil
	}
	return absPath, nil
}

func restartServices(cfg config) {
	fmt.Print("\n=== Restarting services ===\n\n")

	services := serviceList(cfg)
	if len(services) == 0 {
		fmt.Println("No services configured")
		return
	}
	for _, svc := range services {
		fmt.Printf("Restarting %s...\n", svc)
		cmd := fmt.Sprintf("systemctl restart %s", svc)
		if _, err := runRemoteCommand(cfg, cmd); err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			fmt.Println("  OK")
		}
	}

	fmt.Println()
	showStatus(cfg)
}

func reloadServices(cfg config) {
	fmt.Print("\n=== Reloading services ===\n\n")

	services := serviceList(cfg)
	if len(services) == 0 {
		fmt.Println("No services configured")
		return
	}
	for _, svc := range services {
		fmt.Printf("Reloading %s...\n", svc)
		cmd := fmt.Sprintf("systemctl reload-or-restart %s", svc)
		if _, err := runRemoteCommand(cfg, cmd); err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			fmt.Println("  OK")
		}
	}

	fmt.Println()
	showStatus(cfg)
}

func showStatus(cfg config) {
	fmt.Print("\n=== Service Status ===\n\n")

	services := serviceList(cfg)
	if len(services) == 0 {
		fmt.Println("No services configured")
		return
	}
	for _, svc := range services {
		fmt.Printf("--- %s ---\n", svc)
		cmd := fmt.Sprintf("systemctl status %s --no-pager || true", svc)
		if output, err := runRemoteCommand(cfg, cmd); err == nil {
			fmt.Println(output)
		} else {
			fmt.Printf("Error: %v\n", err)
		}
		fmt.Println()
	}
}

func showLogs(cfg config, lines int) {
	fmt.Printf("\n=== Recent Logs (last %d lines) ===\n\n", lines)

	services := serviceList(cfg)
	if len(services) == 0 {
		fmt.Println("No services configured")
		return
	}
	for _, svc := range services {
		fmt.Printf("--- %s ---\n", svc)
		cmd := fmt.Sprintf("journalctl -u %s -n %d --no-pager || true", svc, lines)
		if output, err := runRemoteCommand(cfg, cmd); err == nil {
			fmt.Println(output)
		} else {
			fmt.Printf("Error: %v\n", err)
		}
		fmt.Println()
	}
}

func followLogs(cfg config) {
	fmt.Print("\n=== Following Logs (Ctrl+C to stop) ===\n\n")

	services := serviceList(cfg)
	if len(services) == 0 {
		fmt.Println("No services configured")
		return
	}
	var sb strings.Builder
	sb.WriteString("journalctl")
	for _, svc := range services {
		sb.WriteString(" -u ")
		sb.WriteString(svc)
	}
	sb.WriteString(" -f --no-pager")

	if err := runRemoteCommandStreaming(cfg, sb.String()); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func stopServices(cfg config) {
	fmt.Print("\n=== Stopping services ===\n\n")

	services := serviceList(cfg)
	if len(services) == 0 {
		fmt.Println("No services configured")
		return
	}
	for _, svc := range services {
		fmt.Printf("Stopping %s...\n", svc)
		cmd := fmt.Sprintf("systemctl stop %s", svc)
		if _, err := runRemoteCommand(cfg, cmd); err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			fmt.Println("  OK")
		}
	}

	fmt.Println()
	showStatus(cfg)
}

func removeServiceInteractive(cfg config, reader *bufio.Reader) {
	fmt.Print("\n=== Remove Service ===\n\n")
	fmt.Println("Select service to remove:")
	fmt.Printf("  1) %s (arbitrage)\n", serviceNameForCommand(cfg, "arbitrage"))
	fmt.Printf("  2) %s (arbitrage-equal)\n", serviceNameForCommand(cfg, "arbitrage-equal"))
	fmt.Printf("  3) %s (arbitrage-weighted)\n", serviceNameForCommand(cfg, "arbitrage-weighted"))
	fmt.Println("  0) Cancel")
	fmt.Print("\nSelect option: ")

	input, err := reader.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			fmt.Printf("Error reading input: %v\n", err)
		}
		return
	}

	choice := strings.TrimSpace(strings.ToLower(input))
	switch choice {
	case "1", "arbitrage":
		removeService(cfg, "arbitrage")
	case "2", "equal", "arbitrage-equal":
		removeService(cfg, "arbitrage-equal")
	case "3", "weighted", "arbitrage-weighted":
		removeService(cfg, "arbitrage-weighted")
	case "0", "cancel", "q", "quit", "exit":
		fmt.Println("Canceled.")
	default:
		fmt.Printf("Unknown option: %s\n", choice)
	}
}

func removeService(cfg config, cmdName string) {
	serviceName := serviceNameForCommand(cfg, cmdName)
	if serviceName == "" {
		fmt.Println("No service configured for:", cmdName)
		return
	}

	fmt.Print("\n=== Removing service ===\n\n")
	fmt.Println("This will:")
	fmt.Printf("  - Stop %s\n", serviceName)
	fmt.Printf("  - Disable %s\n", serviceName)
	fmt.Printf("  - Remove /etc/systemd/system/%s.service\n", serviceName)
	fmt.Printf("  - Keep %s directory intact\n", cfg.remoteDir)
	fmt.Println()
	fmt.Print("Are you sure? (yes/no): ")

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading input: %v\n", err)
		return
	}

	if strings.TrimSpace(strings.ToLower(input)) != "yes" {
		fmt.Println("Aborted.")
		return
	}

	fmt.Printf("Stopping %s...\n", serviceName)
	stopCmd := fmt.Sprintf("systemctl stop %s || true", serviceName)
	if _, err := runRemoteCommand(cfg, stopCmd); err != nil {
		fmt.Printf("  Warning: %v\n", err)
	}

	fmt.Printf("Disabling %s...\n", serviceName)
	disableCmd := fmt.Sprintf("systemctl disable %s || true", serviceName)
	if _, err := runRemoteCommand(cfg, disableCmd); err != nil {
		fmt.Printf("  Warning: %v\n", err)
	}

	unitFile := fmt.Sprintf("/etc/systemd/system/%s.service", serviceName)
	fmt.Printf("Removing %s...\n", unitFile)
	rmCmd := fmt.Sprintf("rm -f %s", unitFile)
	if _, err := runRemoteCommand(cfg, rmCmd); err != nil {
		fmt.Printf("  Warning: %v\n", err)
	}

	fmt.Println("Reloading systemd...")
	if _, err := runRemoteCommand(cfg, "systemctl daemon-reload"); err != nil {
		fmt.Printf("  Warning: %v\n", err)
	}

	fmt.Printf("\n=== %s removed successfully ===\n", serviceName)
}

func removeServices(cfg config) {
	fmt.Print("\n=== Removing services ===\n\n")

	fmt.Println("This will:")
	fmt.Println("  - Stop services")
	fmt.Println("  - Disable services")
	fmt.Println("  - Remove systemd unit files")
	fmt.Printf("  - Remove %s directory\n", cfg.remoteDir)
	fmt.Println()
	fmt.Print("Are you sure? (yes/no): ")

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading input: %v\n", err)
		return
	}

	if strings.TrimSpace(strings.ToLower(input)) != "yes" {
		fmt.Println("Aborted.")
		return
	}

	services := serviceList(cfg)
	if len(services) == 0 {
		fmt.Println("No services configured")
		return
	}

	// Stop services
	for _, svc := range services {
		fmt.Printf("Stopping %s...\n", svc)
		cmd := fmt.Sprintf("systemctl stop %s || true", svc)
		if _, err := runRemoteCommand(cfg, cmd); err != nil {
			fmt.Printf("  Warning: %v\n", err)
		}
	}

	// Disable services
	for _, svc := range services {
		fmt.Printf("Disabling %s...\n", svc)
		cmd := fmt.Sprintf("systemctl disable %s || true", svc)
		if _, err := runRemoteCommand(cfg, cmd); err != nil {
			fmt.Printf("  Warning: %v\n", err)
		}
	}

	// Remove unit files
	for _, svc := range services {
		unitFile := fmt.Sprintf("/etc/systemd/system/%s.service", svc)
		fmt.Printf("Removing %s...\n", unitFile)
		cmd := fmt.Sprintf("rm -f %s", unitFile)
		if _, err := runRemoteCommand(cfg, cmd); err != nil {
			fmt.Printf("  Warning: %v\n", err)
		}
	}

	// Reload systemd
	fmt.Println("Reloading systemd...")
	if _, err := runRemoteCommand(cfg, "systemctl daemon-reload"); err != nil {
		fmt.Printf("  Warning: %v\n", err)
	}

	// Remove remote directory
	fmt.Printf("Removing %s...\n", cfg.remoteDir)
	cmd := fmt.Sprintf("rm -rf %s", cfg.remoteDir)
	if _, err := runRemoteCommand(cfg, cmd); err != nil {
		fmt.Printf("  Warning: %v\n", err)
	}

	fmt.Println("\n=== Services removed successfully ===")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func serviceNameForCommand(cfg config, cmdName string) string {
	if strings.TrimSpace(cfg.serviceName) != "" {
		return cfg.serviceName
	}
	switch cmdName {
	case "arbitrage":
		return cfg.arbitrageService
	case "arbitrage-equal":
		return cfg.arbitrageEqualService
	case "arbitrage-weighted":
		return cfg.arbitrageWeightedService
	default:
		return "poly-gocopy-" + cmdName
	}
}

func serviceList(cfg config) []string {
	seen := make(map[string]bool)
	var services []string
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		services = append(services, s)
	}
	add(cfg.arbitrageService)
	add(cfg.arbitrageEqualService)
	add(cfg.arbitrageWeightedService)
	return services
}

func init() {
	// Ensure we're running from repo root or adjust paths
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		log.Println("[warn] This tool is designed for Unix-like systems")
	}
}
