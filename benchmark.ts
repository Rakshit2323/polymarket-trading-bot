import 'dotenv/config';
import { ethers } from 'ethers';
import * as fs from 'fs';
import * as path from 'path';
import Logger from '../utils/logger';

type TxTiming = {
    txHash: string;
    t_logs?: number;
    t_block?: number;
};

class TimingStore {
    private readonly data = new Map<string, TxTiming>();

    upsert(hash: string, patch: Partial<TxTiming>) {
        const key = hash.toLowerCase();
        const prev = this.data.get(key) ?? { txHash: key };
        const next = { ...prev, ...patch } as TxTiming;
        this.data.set(key, next);
    }

    get(hash: string) {
        return this.data.get(hash.toLowerCase());
    }

    completedLogs(): TxTiming[] {
        return Array.from(this.data.values()).filter((t) => t.t_logs);
    }
}

type Args = {
    leader: string;
    polygonWs: string;
    dumpEveryMs: number;
    outFile?: string;
};

const EXCHANGE_ADDRESS = '0xC5d563A36AE78145C45a50134d48A1215220f80a'.toLowerCase();
const EXCHANGE_ABI = [
    'event OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)',
];

const iface = new ethers.utils.Interface(EXCHANGE_ABI);
const store = new TimingStore();
const blockCache = new Map<number, number>(); // blockNumber -> timestamp ms

const parseArgs = (): Args => {
    const argMap = new Map<string, string>();
    process.argv.slice(2).forEach((raw) => {
        const trimmed = raw.startsWith('--') ? raw.slice(2) : raw;
        const [k, v] = trimmed.split('=');
        if (k) argMap.set(k, v ?? 'true');
    });

    const leader = (argMap.get('leader') || process.env.LEADER_PROXY || '').toLowerCase();
    if (!leader) throw new Error('leader proxy address required via --leader=0x... or LEADER_PROXY');

    const envWs = process.env.RPC_WS_URL || process.env.RPC_URL || process.env.POLYGON_WS_URL;
    const polygonWs = argMap.get('polygon-ws') || envWs || 'wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY';

    if (!polygonWs.startsWith('wss')) {
        throw new Error(
            `RPC_WS_URL / polygon-ws must be a WebSocket endpoint (got "${polygonWs}"). Set RPC_WS_URL in .env to your node's wss URL.`
        );
    }

    if (/YOUR_KEY/i.test(polygonWs)) {
        throw new Error(
            'Polygon WS URL still contains placeholder YOUR_KEY. Set RPC_WS_URL (preferred) or use --polygon-ws with a real provider key.'
        );
    }

    return {
        leader,
        polygonWs,
        dumpEveryMs: parseInt(argMap.get('dump-ms') || '10000', 10),
        outFile: argMap.get('out') || argMap.get('outfile') || process.env.LATENCY_OUT_FILE,
    };
};

const startLogsListener = (polygonWs: string, leader: string, onLog: (hash: string, t: number) => void) => {
    const provider = new ethers.providers.WebSocketProvider(polygonWs);
    const orderFilledTopic = iface.getEventTopic('OrderFilled');
    const filter = { address: EXCHANGE_ADDRESS };
    const minDataHexLen = 2 + 64 * 5; // 0x + five uint256 words

    provider.on(filter, (log) => {
        try {
            if (!log.topics || log.topics[0] !== orderFilledTopic) return;
            if (!log.data || log.data.length < minDataHexLen) {
                // avoid ethers parseLog buffer overruns from truncated logs on some public nodes
                return;
            }
            const parsed = iface.parseLog(log);

            const maker = (parsed.args.maker as string).toLowerCase();
            const taker = (parsed.args.taker as string).toLowerCase();
            if (maker !== leader && taker !== leader) return;

            const txHash = (log.transactionHash as string).toLowerCase();
            const now = Date.now();
            onLog(txHash, now);

            // fetch block timestamp async with cache
            if (typeof log.blockNumber === 'number') {
                const cached = blockCache.get(log.blockNumber);
                if (cached) {
                    store.upsert(txHash, { t_block: cached });
                } else {
                    provider
                        .getBlock(log.blockNumber)
                        .then((b) => {
                            if (b?.timestamp) {
                                const tsMs = b.timestamp * 1000;
                                blockCache.set(log.blockNumber, tsMs);
                                store.upsert(txHash, { t_block: tsMs });
                            }
                        })
                        .catch((err) => Logger.warning(`Block fetch failed for ${log.blockNumber}: ${(err as Error).message}`));
                }
            }

            Logger.info(`OrderFilled log for leader tx=${txHash} block=${log.blockNumber}`);
        } catch (err) {
            Logger.warning(`Log parse error: ${(err as Error).message}`);
        }
    });

    provider._websocket?.on('error', (err: any) => {
        Logger.error(`Polygon WS error: ${err?.message || err}`);
    });

    provider._websocket?.on('close', (code: number) => {
        Logger.error(`Polygon WS closed code=${code}`);
    });

    Logger.success(`Subscribed to logs on ${polygonWs}`);
    return provider;
};

const summarize = (values: number[]) => {
    const sorted = [...values].sort((a, b) => a - b);
    const pick = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
    return { min: sorted[0], median: pick(0.5), p95: pick(0.95), max: sorted[sorted.length - 1] };
};

let outPath: string | undefined;

const appendSample = (row: Record<string, number | string | undefined>) => {
    if (!outPath) return;
    try {
        fs.mkdirSync(path.dirname(outPath), { recursive: true });
        fs.appendFileSync(outPath, `${JSON.stringify(row)}\n`, 'utf8');
    } catch (err) {
        Logger.warning(`Failed to write sample to ${outPath}: ${(err as Error).message}`);
    }
};

const dumpCompleted = () => {
    const logs = store.completedLogs();
    if (!logs.length) {
        Logger.info('No OrderFilled logs yet');
        return;
    }

    const withBlock = logs.filter((t) => t.t_block);
    if (withBlock.length) {
        const logsEvt = withBlock.map((t) => t.t_logs! - t.t_block!);
        const logsEvtStats = summarize(logsEvt);
        Logger.info(
            `Δ(logs-event) ms → n=${logsEvt.length}, min ${logsEvtStats.min}, median ${logsEvtStats.median}, p95 ${logsEvtStats.p95}, max ${logsEvtStats.max}`
        );
    } else {
        Logger.info('Waiting for block timestamps to compute event→logs latency');
    }

    logs.forEach((d) => {
        const times = [d.t_logs, d.t_block].filter(Boolean) as number[];
        const t0 = times.length ? Math.min(...times) : undefined;
        const row: Record<string, number | string | undefined> = {
            tx: d.txHash,
            t_logs: d.t_logs,
        };
        if (t0 !== undefined) {
            row.ms_from_first_logs = d.t_logs !== undefined ? d.t_logs - t0 : undefined;
        }
        if (d.t_block) {
            row.t_block = d.t_block;
            row.ms_event_to_logs = d.t_logs !== undefined ? d.t_logs - d.t_block : undefined;
        }
        Logger.info(JSON.stringify(row));
        appendSample(row);
    });
};

const main = async () => {
    const args = parseArgs();
    outPath = args.outFile;
    Logger.header('Benchmark OrderFilled Log Latency (RPC logs)');
    Logger.info(`Leader proxy: ${args.leader}`);

    const onLogs = (hash: string, t: number) => store.upsert(hash, { t_logs: t });

    startLogsListener(args.polygonWs, args.leader, onLogs);

    setInterval(dumpCompleted, args.dumpEveryMs);
};

main().catch((err) => {
    Logger.error(`Fatal: ${(err as Error).message}`);
    process.exit(1);
});
