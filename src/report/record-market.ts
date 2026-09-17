import { mkdirSync, readFileSync, writeFileSync, renameSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
import dotenv from 'dotenv';
import { TinkoffInvestApi } from 'tinkoff-invest-api';
import { InstrumentIdType } from 'tinkoff-invest-api/dist/generated/instruments.js';
import { OrderBookType, TradeSourceType, SubscriptionAction } from 'tinkoff-invest-api/dist/generated/marketdata.js';
import { getTinkoffClientOptions } from '../core/tinkoff-client.js';
import { RecordingWriter, type ClosedRecordingSegment } from '../research/market-recording.js';
import { CheckpointQueue, CheckpointQueueError } from '../research/checkpoint-queue.js';
import { captureMarketStream } from '../research/market-recorder.js';
import { reportMarketRecording } from '../research/market-recording-report.js';
import { captureDeadlineSignal, classifyMetadataError, MetadataRequestFailure, retryMetadata, type MetadataDiagnostic, type MetadataOperation } from './metadata-retry.js';
import { nextMainSessionWindow } from '../research/observation-session.js';
import { normalizeIntervals, observationSubscriptions, subscriptionAcknowledgments, type ObservationManifest, type RequestedSource } from '../research/market-observation.js';

export interface RecorderCheckpoint {
  directory: string;
  segment: ClosedRecordingSegment;
  manifest: ObservationManifest;
}

export interface RecorderArguments {
  seconds: number; tickers: string[]; source: RequestedSource; depth: number;
  budgetRub: number; commissionRate: number; outputDir: string; maxBytes: number;
  segmentMaxBytes?: number; session?: 'any' | 'main';
  captureDeadlineMs?: number;
  parentSignal?: AbortSignal;
  setExitCodeOnFailure?: boolean;
  checkpointIntervalMs?: number;
  onCheckpoint?: (checkpoint: RecorderCheckpoint, signal: AbortSignal) => Promise<void>;
  checkpointMaxPending?: number;
  checkpointUploadTimeoutMs?: number;
  checkpointDrainTimeoutMs?: number;
}

export function boundedCaptureDuration(requestedMs: number, nowMs: number, deadlineMs?: number): number {
  if (!Number.isFinite(requestedMs) || requestedMs <= 0 || !Number.isFinite(nowMs)
    || (deadlineMs !== undefined && !Number.isFinite(deadlineMs))) throw new Error('Invalid capture deadline');
  const duration = Math.min(requestedMs, deadlineMs === undefined ? requestedMs : deadlineMs - nowMs);
  if (duration <= 0) throw new Error('Capture deadline has passed');
  return duration;
}
export function parseRecorderArguments(args: string[], root: string): RecorderArguments {
  const values = new Map<string, string>();
  const allowed = new Set(['--seconds', '--tickers', '--source', '--depth', '--budget-rub', '--commission-pct', '--output-dir', '--max-mb', '--segment-mb', '--session']);
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i], value = args[i + 1];
    if (!allowed.has(key) || !value || value.startsWith('--') || values.has(key)) throw new Error('Invalid or duplicated recorder argument');
    values.set(key, value);
  }
  const number = (key: string, fallback: number, minimum: number, maximum: number, integer = false) => {
    const value = values.has(key) ? Number(values.get(key)) : fallback;
    if (!Number.isFinite(value) || value < minimum || value > maximum || (integer && !Number.isInteger(value))) throw new Error(`Invalid value for ${key}`);
    return value;
  };
  const source = values.get('--source') ?? 'exchange';
  if (!['exchange', 'dealer', 'all'].includes(source)) throw new Error('Invalid source');
  const session = values.get('--session') ?? 'any';
  if (!['any', 'main'].includes(session) || (session === 'main' && source !== 'exchange')) throw new Error('Main session requires exchange source');
  const maxMb = number('--max-mb', 256, 1, 1024, true);
  const segmentMaxBytes = Math.floor(number('--segment-mb', Math.min(32, maxMb), .01, maxMb) * 1024 * 1024);
  const depth = number('--depth', 20, 1, 50, true);
  if (![1, 10, 20, 30, 40, 50].includes(depth)) throw new Error('Invalid depth');
  const tickers = (values.get('--tickers') ?? 'SBER,GAZP,MAGN,VKCO,SMLT,AFKS').split(',').map(x => x.trim().toUpperCase());
  if (!tickers.length || tickers.length > 12 || new Set(tickers).size !== tickers.length || tickers.some(x => !/^[A-Z0-9]{1,12}$/.test(x))) throw new Error('Invalid or duplicate tickers');
  return {
    seconds: number('--seconds', 60, 1, 86400, true), tickers, source: source as RequestedSource, depth,
    budgetRub: number('--budget-rub', 4000, 1, 100000), commissionRate: number('--commission-pct', .05, 0, 10) / 100,
    outputDir: path.resolve(root, values.get('--output-dir') ?? 'data/market-observations'),
    maxBytes: maxMb * 1024 * 1024, segmentMaxBytes, session: session as 'any' | 'main',
  };
}

export function captureCompletionReason(reason: string, userAborted: boolean, nowMs: number, deadlineMs?: number): string {
  // The absolute wall-clock guard starts before writer setup; it may win the stream's later relative timer.
  return reason === 'aborted' && !userAborted && deadlineMs !== undefined && nowMs >= deadlineMs ? 'duration' : reason;
}

/** Normalize the actual terminal event before it becomes immutable/hash-bound. */
export function normalizeCaptureStop(payload: unknown, userAborted: boolean, nowMs: number, deadlineMs?: number): { reason: string; [key: string]: unknown } {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload) || typeof (payload as { reason?: unknown }).reason !== 'string') {
    throw new Error('Invalid capture stop payload');
  }
  const stop = payload as { reason: string; [key: string]: unknown };
  return { ...stop, reason: captureCompletionReason(stop.reason, userAborted, nowMs, deadlineMs) };
}

export function recorderFailure(error: unknown, stage: string): {
  code: number | null; stage: string; category: string; retryable: boolean;
  operation?: MetadataOperation; attempt?: number;
} {
  if (error instanceof CheckpointQueueError) return { code: null, stage: 'checkpoint', category: error.category, retryable: false };
  const diagnostic = error instanceof MetadataRequestFailure ? error.diagnostic : classifyMetadataError(error);
  const retryable = stage === 'metadata' && ['TIMEOUT', 'UNAVAILABLE', 'RESOURCE_EXHAUSTED'].includes(diagnostic.classification);
  return { code: diagnostic.code, stage, category: diagnostic.classification, retryable,
    ...(error instanceof MetadataRequestFailure ? { operation: error.diagnostic.operation, attempt: error.diagnostic.attempt } : {}) };
}

class ObservationApi extends TinkoffInvestApi { close(): void { this.channel.close(); } }
function writeJson(file: string, value: unknown): void {
  const temporary = `${file}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  renameSync(temporary, file);
}

export async function recordMarket(args: RecorderArguments, root: string): Promise<string> {
  if (args.onCheckpoint && args.checkpointIntervalMs === undefined && args.segmentMaxBytes === undefined) {
    throw new Error('Checkpoint capture requires temporal or size segmentation');
  }
  boundedCaptureDuration(args.seconds * 1000, Date.now(), args.captureDeadlineMs);
  // Do not import application config: it starts unrelated validation/logging and can exit the process.
  const local = existsSync(path.join(root, '.env')) ? dotenv.parse(readFileSync(path.join(root, '.env'))) : {};
  const tokenSettings = { ...local, ...process.env };
  const options = getTinkoffClientOptions({ IS_SANDBOX: true, EXECUTION_MODE: 'SANDBOX',
    TINKOFF_API_TOKEN: tokenSettings.TINKOFF_API_TOKEN, TINKOFF_API_TOKEN_SANDBOX: tokenSettings.TINKOFF_API_TOKEN_SANDBOX });
  const runId = randomUUID();
  const directory = path.join(args.outputDir, `${new Date().toISOString().replaceAll(':', '-')}-${runId.slice(0, 8)}`);
  mkdirSync(args.outputDir, { recursive: true }); mkdirSync(directory, { mode: 0o700 });
  const files = ['src/report/record-market.ts', 'src/report/metadata-retry.ts', 'src/research/market-recorder.ts', 'src/research/market-recording.ts',
    'src/research/market-observation.ts', 'src/research/checkpoint-queue.ts', 'src/research/market-recording-report.ts', 'src/research/order-book-costs.ts',
    'src/research/observation-session.ts', 'src/core/tinkoff-client.ts', 'certs/russian-trusted-root-ca.pem', 'package-lock.json'];
  const codeHashes = Object.fromEntries(files.map(file => [file, createHash('sha256').update(readFileSync(path.join(root, file))).digest('hex')]));
  const manifest: ObservationManifest = {
    schemaVersion: 1, runId, createdAt: new Date().toISOString(), status: 'PREPARING', endpoint: options.endpoint,
    source: args.source, instruments: [], intervals: [], scheduleFetchedAt: null, codeHashes,
    settings: { durationMs: args.seconds * 1000, depth: args.depth, budgetRub: args.budgetRub, commissionRate: args.commissionRate,
      maxBookAgeMs: 2000, maxFutureSkewMs: 1000, sampleIntervalMs: 1000, maxBytes: args.maxBytes, fsyncEveryMs: 1000,
      subscriptionTimeoutMs: 10000, heartbeatTimeoutMs: 20000, maxReconnects: 5,
      segmentMaxBytes: args.segmentMaxBytes, session: args.session ?? 'any' },
    notes: [
      'Read-only sandbox market data; no orders, account creation, funding or portfolio access.',
      'Decoded SDK payloads retain units/nano. SDK exchange timestamps have millisecond precision; original protobuf bytes are not retained.',
      'Commission is an explicit research scenario, default sandbox fee0.05% per side; no live account tariff claim.',
      'Source EXCHANGE/DEALER comes from each event; sourceALL or UNKNOWN is not assumed to be exchange.',
      'Only API-provided schedule intervals are classified. No hard-coded historical main-session start.',
      'Repeated trade observations are retained because the SDK trade has no unique trade ID.',
      'This finite run does not schedule future collection or start any trading strategy.',
    ],
  };
  if (args.checkpointIntervalMs !== undefined) manifest.notes.push(`Closed immutable checkpoint interval: ${args.checkpointIntervalMs}ms; one continuous run.`);
  const manifestFile = path.join(directory, 'manifest.json'); writeJson(manifestFile, manifest);
  const api = new ObservationApi(options), controller = new AbortController();
  const stop = () => controller.abort();
  // A terminal and an npm parent can both forward the same interrupt. Keep cleanup idempotent.
  process.on('SIGINT', stop); process.on('SIGTERM', stop);
  const onParentAbort = () => controller.abort(args.parentSignal?.reason);
  args.parentSignal?.addEventListener('abort', onParentAbort, { once: true });
  if (args.parentSignal?.aborted) onParentAbort();
  const captureBound = captureDeadlineSignal(args.captureDeadlineMs, controller.signal);
  let writer: RecordingWriter | undefined, stage = 'metadata';
  let checkpointQueue: CheckpointQueue<RecorderCheckpoint> | undefined;
  const metadataDiagnostics: MetadataDiagnostic[] = [];
  const metadata = <T>(operation: MetadataOperation, request: (signal: AbortSignal) => Promise<T>) => retryMetadata(request, {
    operation, signal: controller.signal, deadlineMs: args.captureDeadlineMs,
    onDiagnostic: diagnostic => {
      metadataDiagnostics.push(diagnostic);
      writeJson(path.join(directory, 'metadata-retries.json'), metadataDiagnostics);
    },
  });
  try {
    checkpointQueue = args.onCheckpoint ? new CheckpointQueue<RecorderCheckpoint>({
      upload: args.onCheckpoint, onFailure: error => controller.abort(error),
      maxPending: args.checkpointMaxPending, uploadTimeoutMs: args.checkpointUploadTimeoutMs,
    }) : undefined;
    const shares = [];
    for (const ticker of args.tickers) {
      const { instrument: i } = await metadata('shareBy', signal => api.instruments.shareBy({ idType: InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER, id: ticker, classCode: 'TQBR' }, { signal }));
      if (!i || i.ticker !== ticker || i.classCode !== 'TQBR' || i.currency.toUpperCase() !== 'RUB'
        || !i.apiTradeAvailableFlag || !i.uid || !Number.isSafeInteger(i.lot) || i.lot <= 0) throw new Error('Unsupported or mismatched instrument');
      shares.push(i);
      manifest.instruments.push({ instrumentId: i.uid, uid: i.uid, figi: i.figi, ticker: i.ticker, classCode: i.classCode,
        lot: i.lot, currency: i.currency, name: i.name, exchange: i.exchange, sector: i.sector });
    }
    writeJson(path.join(directory, 'instrument-responses.json'), shares);
    const now = new Date();
    const schedules = await metadata('tradingSchedules', signal => api.instruments.tradingSchedules({ from: now, to: new Date(now.getTime() + 2 * 86400000) }, { signal }));
    manifest.scheduleFetchedAt = new Date().toISOString();
    // Save the exact calendars belonging to this run, preserving all fields for future interpretation.
    const exchanges = new Set(manifest.instruments.map(i => i.exchange.toUpperCase()));
    const relevant = { exchanges: schedules.exchanges.filter(s => exchanges.has(s.exchange.toUpperCase())) };
    writeJson(path.join(directory, 'schedule-response.json'), relevant);
    manifest.intervals = normalizeIntervals(relevant, [...exchanges]);
    const statuses = await metadata('getTradingStatuses', signal => api.marketdata.getTradingStatuses({ instrumentId: manifest.instruments.map(i => i.uid) }, { signal }));
    writeJson(path.join(directory, 'initial-trading-statuses.json'), { receivedAt: new Date().toISOString(), response: statuses });
    const checkedAt = Date.now();
    manifest.settings.durationMs = boundedCaptureDuration(manifest.settings.durationMs, checkedAt, args.captureDeadlineMs);
    if (args.captureDeadlineMs !== undefined) manifest.notes.push(`Absolute capture deadline: ${new Date(args.captureDeadlineMs).toISOString()}`);
    if (args.session === 'main') {
      if (args.source !== 'exchange') throw new Error('Main session requires exchange source');
      manifest.nextSession = nextMainSessionWindow(manifest.instruments, manifest.intervals, checkedAt, manifest.settings.durationMs);
      if (!manifest.nextSession || Date.parse(manifest.nextSession.start) > checkedAt) {
        manifest.status = 'WAITING_FOR_MAIN_SESSION'; writeJson(manifestFile, manifest);
        console.log(JSON.stringify({ directory, status: manifest.status, nextSession: manifest.nextSession }, null, 2));
        return directory;
      }
    }
    manifest.status = 'RECORDING'; writeJson(manifestFile, manifest);
    stage = 'stream';
    // All checkpoints use the same frozen metadata; final status/hash are committed only after capture.
    const checkpointManifest = structuredClone(manifest);
    writer = new RecordingWriter({ path: path.join(directory, 'events.ndjson'), runId, maxBytes: args.maxBytes,
      segmentMaxBytes: args.segmentMaxBytes, fsyncEveryMs: manifest.settings.fsyncEveryMs,
      checkpointIntervalMs: args.checkpointIntervalMs,
      onSegmentClosed: checkpointQueue ? segment => checkpointQueue!.enqueue({ directory, segment,
        manifest: structuredClone(checkpointManifest) }) : undefined });
    const source = args.source === 'exchange' ? OrderBookType.ORDERBOOK_TYPE_EXCHANGE : args.source === 'dealer' ? OrderBookType.ORDERBOOK_TYPE_DEALER : OrderBookType.ORDERBOOK_TYPE_ALL;
    const tapeSource = args.source === 'exchange' ? TradeSourceType.TRADE_SOURCE_EXCHANGE : args.source === 'dealer' ? TradeSourceType.TRADE_SOURCE_DEALER : TradeSourceType.TRADE_SOURCE_ALL;
    const subscribe = SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE;
    const request = {
      subscribeOrderBookRequest: { subscriptionAction: subscribe, instruments: manifest.instruments.map(i => ({ instrumentId: i.uid, figi: '', depth: args.depth, orderBookType: source })) },
      subscribeTradesRequest: { subscriptionAction: subscribe, instruments: manifest.instruments.map(i => ({ instrumentId: i.uid, figi: '' })), tradeSource: tapeSource },
      subscribeInfoRequest: { subscriptionAction: subscribe, instruments: manifest.instruments.map(i => ({ instrumentId: i.uid, figi: '' })) },
      pingSettings: { pingDelayMs: 5000 },
    };
    const activeWriter = writer;
    let recordedStopReason: string | undefined;
    manifest.capture = await captureMarketStream({
      openStream: async function* (signal) {
        const response = await api.marketdata.getTradingStatuses({ instrumentId: manifest.instruments.map(i => i.uid) },
          { signal: AbortSignal.any([signal, AbortSignal.timeout(5000)]) });
        // A separate, explicitly labelled SDK unary response, captured in the same hash-bound epoch.
        yield { observationOrigin: 'UNARY_GET_TRADING_STATUSES', response };
        yield* api.marketdataStream.marketDataServerSideStream(request, { signal });
      },
      record: (kind, payload, epoch) => {
        checkpointQueue?.throwIfFailed();
        if (kind === 'stop') {
          const stopPayload = normalizeCaptureStop(payload, controller.signal.aborted, Date.now(), args.captureDeadlineMs);
          activeWriter.append(kind, stopPayload, epoch);
          recordedStopReason = stopPayload.reason;
        } else activeWriter.append(kind, payload, epoch);
      },
      expectedSubscriptions: observationSubscriptions(manifest.instruments),
      acknowledgments: payload => subscriptionAcknowledgments(payload, args.source, args.depth),
      durationMs: manifest.settings.durationMs, subscriptionTimeoutMs: manifest.settings.subscriptionTimeoutMs,
      heartbeatTimeoutMs: manifest.settings.heartbeatTimeoutMs, tickIntervalMs: manifest.settings.sampleIntervalMs,
      maxReconnects: manifest.settings.maxReconnects, backoffMs: [1000, 2000, 5000, 10000, 20000], signal: captureBound.signal,
    });
    // Use the reason already committed to raw storage; later aborts must not change its meaning.
    manifest.capture.reason = recordedStopReason ?? manifest.capture.reason;
    manifest.recording = writer.close(); writer = undefined;
    if (checkpointQueue) { stage = 'checkpoint'; await checkpointQueue.drain(args.checkpointDrainTimeoutMs); }
    manifest.status = ['duration', 'aborted', 'duration_elapsed', 'external_abort'].includes(manifest.capture.reason.toLowerCase()) ? 'COMPLETE' : 'FAILED';
    manifest.completedAt = new Date().toISOString(); writeJson(manifestFile, manifest);
    stage = 'report';
    const summary = await reportMarketRecording(directory);
    console.log(JSON.stringify({ directory, captureReason: manifest.capture.reason, status: manifest.status,
      subscriptions: `${summary.successfulSubscriptions.length}/${summary.expectedSubscriptions.length}`, books: summary.bookEvents,
      trades: summary.tradeEvents, exchangeSamples: summary.exchangeSamples, dealerSamples: summary.dealerSamples,
      evidence: summary.evidence }, null, 2));
    if (manifest.status === 'FAILED' && args.setExitCodeOnFailure !== false) process.exitCode = 1;
  } catch (error) {
    controller.abort();
    if (writer) { try { manifest.recording = writer.close(); } catch { /* Integrity will be checked from the partial file. */ } }
    if (checkpointQueue) { try { await checkpointQueue.drain(args.checkpointDrainTimeoutMs); } catch { /* Preserve the original acquisition/upload failure. */ } }
    manifest.status = 'FAILED'; manifest.completedAt = new Date().toISOString();
    manifest.failure = recorderFailure(error, stage); writeJson(manifestFile, manifest);
    if (existsSync(path.join(directory, 'events.ndjson')) && stage !== 'report') {
      try { await reportMarketRecording(directory); } catch { /* Retain the original recording and manifest for diagnosis. */ }
    }
    console.error(JSON.stringify({ status: 'FAILED', ...manifest.failure, directory }));
    if (args.setExitCodeOnFailure !== false) process.exitCode = 1;
  } finally {
    controller.abort(); captureBound.dispose(); api.close();
    args.parentSignal?.removeEventListener('abort', onParentAbort);
    process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop);
  }
  return directory;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2), root = process.cwd();
  if (args[0] === '--help') {
    console.log('Read-only sandbox market observation. --seconds 60 --tickers SBER,GAZP,MAGN,VKCO,SMLT,AFKS --source exchange|dealer|all --session any|main --depth 20 --budget-rub 4000 --commission-pct 0.05 --max-mb 256 --segment-mb 32 --output-dir data/market-observations\nMain session checks the current API calendar and exits with WAITING_FOR_MAIN_SESSION outside a full exchange main-session window. max-mb bounds the whole run; segment-mb rotates files without reconnecting.\nRebuild a saved report: --report /absolute/run/directory'); return;
  }
  if (args[0] === '--report') {
    if (args.length !== 2) throw new Error('One report directory is required');
    const directory = path.resolve(args[1]); await reportMarketRecording(directory); console.log(path.join(directory, 'REPORT.md')); return;
  }
  await recordMarket(parseRecorderArguments(args, root), root);
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(() => { console.error('Market observation failed: check arguments, token availability, and the saved manifest.'); process.exitCode = 1; });
}
