package dotenv

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func Load() error {
	if err := godotenv.Load(); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("load .env: %w", err)
	}
	return nil
}
