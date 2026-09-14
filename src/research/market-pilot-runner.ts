import { createHash, randomUUID } from 'node:crypto';
import {
  closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync, writeSync,
} from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';
import { TinkoffInvestApi } from 'tinkoff-invest-api';
import { InstrumentIdType } from 'tinkoff-invest-api/dist/generated/instruments.js';
import { getTinkoffClientOptions } from '../core/tinkoff-client.js';
import { recordMarket, type RecorderArguments } from '../report/record-market.js';
import { normalizeIntervals, type ObservationInstrument, type ObservationInterval } from './market-observation.js';
import { nextMainSessionWindow } from './observation-session.js';
import { writePilotQualityReport } from './market-pilot-quality.js';

export const PILOT_TICKERS = ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'] as const;
export const PILOT_DURATION_MS = 30 * 60 * 1_000;
export const PILOT_START_MARGIN_MS = 5 * 60 * 1_000;
const DISCOVERY_INTERVAL_MS = 15 * 60 * 1_000;
const MAX_WAIT_MS = 72 * 60 * 60 * 1_000;
const RPC_TIMEOUT_MS = 10_000;

export type MarketPilotStatus = 'WAITING' | 'RECORDING' | 'COMPLETE' | 'FAILED' | 'STOPPED';
export interface MarketPilotState {
  schemaVersion: 1;
  pid: number;
  status: MarketPilotStatus;
  nextStartAt: string | null;
  updatedAt: string;
  recordingDirectory: string | null;
  summaryPath: string | null;
  qualityStatus?: 'PASS' | 'INSUFFICIENT_DATA';
  qualityReportPath?: string;
  detail?: string;
}

export interface PilotDiscovery {
  instruments: ObservationInstrument[];
  intervals: ObservationInterval[];
  fetchedAt: string;
}

export interface MarketPilotDependencies {
  now: () => number;
  sleep: (milliseconds: number, signal: AbortSignal) => Promise<void>;
  discover: (atMs: number, signal: AbortSignal) => Promise<PilotDiscovery>;
  capture: (arguments_: RecorderArguments, root: string) => Promise<string>;
  sourceHashes: () => Record<string, string>;
  close?: () => void;
}

export interface RunMarketPilotOptions {
  root: string;
  stateDir: string;
  signal?: AbortSignal;
  pid?: number;
  maxWaitMs?: number;
  discoveryIntervalMs?: number;
}

type PlanningApi = Pick<TinkoffInvestApi, 'instruments'>;

function atomicJson(file: string, value: unknown): void {
  const temporary = `${file}.${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600, flush: true });
  renameSync(temporary, file);
}

function stateMarkdown(state: MarketPilotState): string {
  const label: Record<MarketPilotStatus, string> = {
    WAITING: 'Ожидание основной сессии', RECORDING: 'Запись рынка', COMPLETE: 'Завершено',
    FAILED: 'Ошибка', STOPPED: 'Остановлено',
  };
  const moscow = (iso: string) => `${new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Moscow', dateStyle: 'long', timeStyle: 'medium',
  }).format(new Date(iso))} МСК`;
  const report = state.qualityReportPath
    ? `\n[Качество данных и расходы](<${state.qualityReportPath}>)\n` : '';
  const summary = state.summaryPath ? `\n[Подробная сводка](<${state.summaryPath}>)\n` : '';
  return `# Биржевой пилот\n\n**${label[state.status]}** (${state.status}). ${state.detail ?? ''}\n\n- План начала записи: ${state.nextStartAt ? moscow(state.nextStartAt) : 'уточняется'}\n- Последнее обновление: ${moscow(state.updatedAt)}\n- Процесс: ${state.pid}\n\nОдин 30-минутный сбор биржевых данных шести акций через sandbox. Программа сама проверяет календарь, записывает данные, формирует отчёт и завершает работу. Агент и торговые заявки в сборе не участвуют.\n\nПока программа работает, Mac должен быть включён и доступен в сети. Чат можно закрыть; после выхода из учётной записи или перезагрузки нужен ручной запуск. Остановка: \`npm run observe:pilot -- stop\`.\n${report}${summary}`;
}

function writeState(stateDir: string, state: MarketPilotState): void {
  atomicJson(path.join(stateDir, 'state.json'), state);
  const temporary = path.join(stateDir, `STATUS.md.${process.pid}.tmp`);
  writeFileSync(temporary, stateMarkdown(state), { mode: 0o600, flush: true });
  renameSync(temporary, path.join(stateDir, 'STATUS.md'));
}

function readState(stateDir: string): MarketPilotState | null {
  const file = path.join(stateDir, 'state.json');
  if (!existsSync(file)) return null;
  const state = JSON.parse(readFileSync(file, 'utf8')) as MarketPilotState;
  if (state.schemaVersion !== 1 || !['WAITING', 'RECORDING', 'COMPLETE', 'FAILED', 'STOPPED'].includes(state.status)) {
    throw new Error('Pilot state is invalid');
  }
  return state;
}

function acquireLock(stateDir: string): { token: string; release: () => void } {
  const file = path.join(stateDir, 'runner.lock');
  const token = `${process.pid}:${randomUUID()}`;
  let descriptor: number;
  try {
    descriptor = openSync(file, 'wx', 0o600);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    // Only ESRCH proves a dead owner; malformed/foreign/permission-denied locks stay intact.
    const isDeadOwner = (): boolean => {
      if (!existsSync(file)) return false;
      const match = /^(\d+):[a-f0-9-]{36}$/.exec(readFileSync(file, 'utf8').trim());
      const owner = match ? Number(match[1]) : 0;
      if (!Number.isSafeInteger(owner) || owner <= 0) return false;
      try { process.kill(owner, 0); return false; }
      catch (probeError) { return (probeError as NodeJS.ErrnoException).code === 'ESRCH'; }
    };
    if (!isDeadOwner()) throw error;
    // Serialize reclaimers. A crash during this short repair leaves the guard in place
    // for manual inspection rather than allowing a second unsafe lock deletion.
    const recoveryFile = path.join(stateDir, 'runner-recovery.lock');
    const recovery = openSync(recoveryFile, 'wx', 0o600);
    try {
      if (!isDeadOwner()) throw error;
      unlinkSync(file);
      descriptor = openSync(file, 'wx', 0o600);
    } finally {
      closeSync(recovery);
      unlinkSync(recoveryFile);
    }
  }
  try {
    const bytes = Buffer.from(`${token}\n`);
    let offset = 0;
    while (offset < bytes.length) {
      const written = writeSync(descriptor, bytes, offset, bytes.length - offset, null);
      if (written <= 0) throw new Error('Pilot lock write made no progress');
      offset += written;
    }
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  return {
    token,
    release: () => {
      try {
        if (readFileSync(file, 'utf8').trim() === token) unlinkSync(file);
      } catch { /* A missing or foreign lock is never removed. */ }
    },
  };
}

function stableHashes(value: Record<string, string>): string {
  return JSON.stringify(Object.entries(value).sort(([left], [right]) => left.localeCompare(right)));
}

function abortableSleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(done, milliseconds);
    const onAbort = (): void => done();
    function done(): void {
      clearTimeout(timer);
      signal.removeEventListener('abort', onAbort);
      resolve();
    }
    signal.addEventListener('abort', onAbort, { once: true });
    if (signal.aborted) done();
  });
}

export function planMarketPilotStart(discovery: PilotDiscovery, nowMs: number): { start: string; end: string } | null {
  const window = nextMainSessionWindow(
    discovery.instruments,
    discovery.intervals,
    nowMs - PILOT_START_MARGIN_MS,
    PILOT_DURATION_MS + PILOT_START_MARGIN_MS,
  );
  if (!window) return null;
  const startMs = Math.max(nowMs, Date.parse(window.start) + PILOT_START_MARGIN_MS);
  return { start: new Date(startMs).toISOString(), end: window.end };
}

export async function discoverMarketPilot(
  api: PlanningApi,
  tickers: readonly string[],
  atMs: number,
  signal: AbortSignal,
): Promise<PilotDiscovery> {
  const instruments: ObservationInstrument[] = [];
  for (const ticker of tickers) {
    const response = await api.instruments.shareBy(
      { idType: InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER, id: ticker, classCode: 'TQBR' },
      { signal: AbortSignal.any([signal, AbortSignal.timeout(RPC_TIMEOUT_MS)]) },
    );
    const instrument = response.instrument;
    if (!instrument || instrument.ticker !== ticker || instrument.classCode !== 'TQBR'
      || instrument.currency.toUpperCase() !== 'RUB' || !instrument.apiTradeAvailableFlag || !instrument.uid
      || !Number.isSafeInteger(instrument.lot) || instrument.lot <= 0 || !instrument.exchange) {
      throw new Error('Pilot instrument metadata is unsupported or mismatched');
    }
    instruments.push({
      instrumentId: instrument.uid, uid: instrument.uid, figi: instrument.figi, ticker: instrument.ticker,
      classCode: instrument.classCode, lot: instrument.lot, currency: instrument.currency, name: instrument.name,
      exchange: instrument.exchange, sector: instrument.sector,
    });
  }
  const schedules = await api.instruments.tradingSchedules(
    { from: new Date(atMs), to: new Date(atMs + 4 * 86_400_000) },
    { signal: AbortSignal.any([signal, AbortSignal.timeout(RPC_TIMEOUT_MS)]) },
  );
  const exchanges = [...new Set(instruments.map((instrument) => instrument.exchange.toUpperCase()))];
  return {
    instruments,
    intervals: normalizeIntervals(schedules, exchanges),
    fetchedAt: new Date(atMs).toISOString(),
  };
}

function inspectCapture(directory: string): { status: string; summaryPath: string | null } {
  const manifestFile = path.join(directory, 'manifest.json');
  if (!existsSync(manifestFile)) return { status: 'MISSING', summaryPath: null };
  const manifest = JSON.parse(readFileSync(manifestFile, 'utf8')) as { status?: unknown };
  const summaryPath = path.join(directory, 'summary.json');
  return { status: typeof manifest.status === 'string' ? manifest.status : 'INVALID', summaryPath: existsSync(summaryPath) ? summaryPath : null };
}

function pilotArguments(stateDir: string): RecorderArguments {
  return {
    seconds: PILOT_DURATION_MS / 1_000,
    tickers: [...PILOT_TICKERS],
    source: 'exchange',
    depth: 20,
    budgetRub: 4_000,
    commissionRate: 0.0005,
    outputDir: path.join(stateDir, 'recordings'),
    maxBytes: 1024 * 1024 * 1024,
    segmentMaxBytes: 32 * 1024 * 1024,
    session: 'main',
  };
}

export async function runMarketPilot(
  options: RunMarketPilotOptions,
  dependencies: MarketPilotDependencies,
): Promise<MarketPilotState> {
  const root = path.resolve(options.root);
  const stateDir = path.resolve(options.stateDir);
  if (!path.isAbsolute(options.stateDir)) throw new Error('--state-dir must be absolute');
  const maxWaitMs = options.maxWaitMs ?? MAX_WAIT_MS;
  const discoveryInterval = options.discoveryIntervalMs ?? DISCOVERY_INTERVAL_MS;
  if (!Number.isSafeInteger(maxWaitMs) || maxWaitMs <= 0
    || !Number.isSafeInteger(discoveryInterval) || discoveryInterval <= 0) {
    throw new Error('Pilot wait bounds must be positive safe integers');
  }
  mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  const prior = readState(stateDir);
  const completedMarker = path.join(stateDir, 'completed.json');
  if (existsSync(completedMarker) || prior?.status === 'COMPLETE') {
    if (!prior) throw new Error('Pilot completion marker exists without readable state');
    return prior;
  }
  const lock = acquireLock(stateDir);
  const controller = new AbortController();
  const forwardAbort = (): void => controller.abort();
  options.signal?.addEventListener('abort', forwardAbort, { once: true });
  if (options.signal?.aborted) controller.abort();
  const signal = controller.signal;
  const pid = options.pid ?? process.pid;
  const startedAt = dependencies.now();
  const deadline = startedAt + maxWaitMs;
  const sourceFile = path.join(stateDir, 'source-hashes.json');
  const captureMarker = path.join(stateDir, 'capture-started.json');
  let state: MarketPilotState = {
    schemaVersion: 1, pid, status: 'WAITING', nextStartAt: null,
    updatedAt: new Date(startedAt).toISOString(), recordingDirectory: null, summaryPath: null,
  };
  writeState(stateDir, state);
  try {
    if (existsSync(captureMarker)) {
      state = { ...state, status: 'FAILED', detail: 'Предыдущая попытка записи началась, но не имеет подтверждённого завершения.', updatedAt: new Date(dependencies.now()).toISOString() };
      writeState(stateDir, state);
      return state;
    }
    const currentHashes = dependencies.sourceHashes();
    const baselineHashes = existsSync(sourceFile)
      ? JSON.parse(readFileSync(sourceFile, 'utf8')) as Record<string, string>
      : currentHashes;
    if (!existsSync(sourceFile)) atomicJson(sourceFile, baselineHashes);
    if (stableHashes(currentHashes) !== stableHashes(baselineHashes)) {
      state = { ...state, status: 'FAILED', detail: 'Исходники сборщика изменились после подготовки запуска.' };
      writeState(stateDir, state);
      return state;
    }

    while (true) {
      const now = dependencies.now();
      if (signal.aborted) {
        state = { ...state, status: 'STOPPED', updatedAt: new Date(now).toISOString(), detail: 'Остановка получена до начала записи.' };
        writeState(stateDir, state);
        return state;
      }
      if (now >= deadline) {
        state = { ...state, status: 'FAILED', updatedAt: new Date(now).toISOString(), detail: 'За ограниченное время не найдено подходящее окно основной сессии.' };
        writeState(stateDir, state);
        return state;
      }

      let discovery: PilotDiscovery;
      try {
        discovery = await dependencies.discover(now, signal);
      } catch {
        if (signal.aborted) continue;
        state = { ...state, status: 'WAITING', nextStartAt: null, updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Календарь временно недоступен; следующая ограниченная проверка через 15 минут.' };
        writeState(stateDir, state);
        await dependencies.sleep(Math.min(discoveryInterval, Math.max(0, deadline - dependencies.now())), signal);
        continue;
      }
      if (signal.aborted || dependencies.now() >= deadline) continue;
      const plan = planMarketPilotStart(discovery, dependencies.now());
      if (!plan || dependencies.now() < Date.parse(plan.start)) {
        const target = plan ? Date.parse(plan.start) : Number.POSITIVE_INFINITY;
        state = {
          ...state, status: 'WAITING', nextStartAt: plan?.start ?? null,
          updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Календарь API проверен; программа ожидает подходящее окно.',
        };
        writeState(stateDir, state);
        const wakeAt = Math.min(target, dependencies.now() + discoveryInterval, deadline);
        await dependencies.sleep(Math.max(0, wakeAt - dependencies.now()), signal);
        continue;
      }

      if (stableHashes(dependencies.sourceHashes()) !== stableHashes(baselineHashes)) {
        state = { ...state, status: 'FAILED', updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Исходники сборщика изменились во время ожидания; запись не запускалась.' };
        writeState(stateDir, state);
        return state;
      }
      atomicJson(captureMarker, { schemaVersion: 1, startedAt: new Date(dependencies.now()).toISOString(), pid });
      state = { ...state, status: 'RECORDING', nextStartAt: plan.start, updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Календарь повторно подтверждён; запись начата.' };
      writeState(stateDir, state);
      const directory = await dependencies.capture(pilotArguments(stateDir), root);
      const observed = inspectCapture(directory);
      if (signal.aborted) {
        state = { ...state, status: 'STOPPED', recordingDirectory: directory, summaryPath: observed.summaryPath, updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Остановка получена; сборщик закрыл доступную запись.' };
        writeState(stateDir, state);
        return state;
      }
      if (observed.status === 'WAITING_FOR_MAIN_SESSION') {
        unlinkSync(captureMarker);
        state = { ...state, status: 'WAITING', recordingDirectory: directory, summaryPath: observed.summaryPath, updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Календарь изменился непосредственно перед записью; окно будет рассчитано заново.' };
        writeState(stateDir, state);
        continue;
      }
      if (observed.status !== 'COMPLETE' || !observed.summaryPath) {
        state = { ...state, status: 'FAILED', recordingDirectory: directory, summaryPath: observed.summaryPath, updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Сборщик вернул каталог без подтверждённого полного результата.' };
        writeState(stateDir, state);
        return state;
      }
      state = { ...state, recordingDirectory: directory, summaryPath: observed.summaryPath };
      const quality = writePilotQualityReport(directory);
      state = { ...state, status: 'COMPLETE', qualityStatus: quality.status, qualityReportPath: quality.reportPath,
        updatedAt: new Date(dependencies.now()).toISOString(),
        detail: quality.status === 'PASS'
          ? 'Запись завершена; рабочие ориентиры качества данных выполнены. Прибыльность стратегии не измерялась.'
          : 'Запись завершена, но данных недостаточно по одному или нескольким ориентирам качества. Причины — в отчёте.' };
      writeState(stateDir, state);
      atomicJson(completedMarker, { schemaVersion: 1, completedAt: state.updatedAt, recordingDirectory: directory });
      return state;
    }
  } catch {
    if (signal.aborted) {
      state = { ...state, status: 'STOPPED', updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Остановка получена; программа завершила доступную операцию.' };
    } else {
      state = { ...state, status: 'FAILED', updatedAt: new Date(dependencies.now()).toISOString(), detail: 'Пилот завершился с ошибкой; подробности следует смотреть в каталоге записи.' };
    }
    writeState(stateDir, state);
    return state;
  } finally {
    controller.abort();
    options.signal?.removeEventListener('abort', forwardAbort);
    try { dependencies.close?.(); } finally { lock.release(); }
  }
}

const guardedSourceFiles = [
  'src/report/record-market.ts', 'src/research/market-recorder.ts', 'src/research/market-recording.ts',
  'src/research/market-observation.ts', 'src/research/market-recording-report.ts', 'src/research/order-book-costs.ts',
  'src/research/observation-session.ts', 'src/core/tinkoff-client.ts', 'package-lock.json',
  'src/research/market-pilot-runner.ts',
  'src/research/market-pilot-quality.ts', 'src/report/run-market-pilot.ts', 'src/report/market-pilot-service.ts',
  'tsconfig.json',
] as const;

export function createProductionMarketPilotDependencies(root: string): MarketPilotDependencies {
  let api: TinkoffInvestApi | null = null;
  const getApi = (): TinkoffInvestApi => {
    if (api) return api;
    const local = existsSync(path.join(root, '.env')) ? dotenv.parse(readFileSync(path.join(root, '.env'))) : {};
    const settings = { ...local, ...process.env };
    api = new TinkoffInvestApi(getTinkoffClientOptions({
      IS_SANDBOX: true, EXECUTION_MODE: 'SANDBOX', TINKOFF_API_TOKEN: settings.TINKOFF_API_TOKEN,
      TINKOFF_API_TOKEN_SANDBOX: settings.TINKOFF_API_TOKEN_SANDBOX,
    }));
    return api;
  };
  return {
    now: () => Date.now(),
    sleep: abortableSleep,
    discover: (atMs, signal) => discoverMarketPilot(getApi(), PILOT_TICKERS, atMs, signal),
    capture: (arguments_, projectRoot) => recordMarket(arguments_, projectRoot),
    sourceHashes: () => Object.fromEntries(guardedSourceFiles.map((file) => [
      file, createHash('sha256').update(readFileSync(path.join(root, file))).digest('hex'),
    ])),
    close: () => {
      if (!api) return;
      const closable = api as TinkoffInvestApi & { channel?: { close(): void } };
      closable.channel?.close();
      api = null;
    },
  };
}
