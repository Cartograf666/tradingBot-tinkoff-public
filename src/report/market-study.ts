import { prepareContinuousCheckpoint, restoreContinuousCheckpoints } from '../research/continuous-checkpoints.js';
import { assessContinuousWindows } from '../research/continuous-windows.js';
import type { ObservationManifest } from '../research/market-observation.js';
import { createHash, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TinkoffInvestApi } from 'tinkoff-invest-api';
import { TINKOFF_SANDBOX_ENDPOINT } from '../core/tinkoff-client.js';
import { confirmImmutableAsset } from './immutable-asset.js';
import { recordMarket, type RecorderArguments } from './record-market.js';
import { closedPlansNeedingFinalization, confirmOperationCheckpoint, continuousOperationFailureStage, continuousRecordingCompleted, markOperationRecordingStopped,
  mergeBlockOperations, operationalDay, renderOperationalDay, recoverableRecordingFailure, recordOwnedSlot, type BlockOperation } from './study-operations.js';
import { readStudyRuntime } from './study-runtime.js';
import { fixedReplayScenarios, replayConfigHash, replayRecording, replaySession, replaySourceHashes } from './replay-orderbook.js';
import { buildDailyResearchReport, dailyResearchIdentity, renderDailyResearchReport, selectDailyResearchInputs } from './daily-research.js';
import { discoverMarketPilot } from '../research/market-pilot-runner.js';
import { nextMainSessionWindow } from '../research/observation-session.js';
import { runSmokeWithRetries, SmokeQualityError, StudyStageError, type SmokeCheckEvent } from './smoke-retry.js';
import {
  STUDY_PROTOCOL_HASH, STUDY_RELEASE_TAG, STUDY_STATE_BRANCH, STUDY_TICKERS, STUDY_LAUNCH_LATENESS_MS, STUDY_CHUNK_END_MARGIN_MS, assertStudyPreparationReady, canonicalJson, chunkDurationSeconds, hashStudyValue, planRecoverableStudyBlock, planStudyBlock, planStudyPreparation,
  type StudyBlock, type StudyBlockPlan, type StudyChunkPlan,
} from '../research/study-protocol.js';
import {
  acceptStudyChunk, acceptStudyDay, beginStudyAttempt, createStudyLedger, freezeStudy, mergeStudyLedgers, planStudyRun,
  type StudyChunkReceipt, type StudyDayReceipt, type StudyLedger,
} from '../research/study-state.js';

type JsonObject = Record<string, unknown>;
type ExecResult = { stdout: Buffer; stderr: Buffer };
const STATE_PATH = 'study-ledger.json';
const README_PATH = 'README.md';

function runFile(command: string, args: string[], options: { cwd?: string; maxBuffer?: number; signal?: AbortSignal } = {}): Promise<ExecResult> {
  return new Promise((resolve, reject) => execFile(command, args, {
    cwd: options.cwd, signal: options.signal, timeout: 90_000, encoding: 'buffer', maxBuffer: options.maxBuffer ?? 512 * 1024 * 1024,
    env: process.env,
  }, (error, stdout, stderr) => error ? reject(Object.assign(error, { stdout, stderr })) : resolve({ stdout, stderr })));
}
function sha256File(file: string): string { return createHash('sha256').update(readFileSync(file)).digest('hex'); }
function atomicJson(file: string, value: unknown): void {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600, flush: true });
}
function output(name: string, value: string): void {
  const file = process.env.GITHUB_OUTPUT;
  if (file) writeFileSync(file, `${name}=${value}\n`, { flag: 'a' });
}
function parseArguments(argv: string[]): { command: string; values: Map<string, string> } {
  const command = argv[0] ?? '';
  const values = new Map<string, string>();
  for (let index = 1; index < argv.length; index += 2) {
    const key = argv[index], value = argv[index + 1];
    if (!key?.startsWith('--') || value === undefined || values.has(key)) throw new Error('Invalid market-study arguments');
    values.set(key, value);
  }
  return { command, values };
}
function required(values: Map<string, string>, key: string): string {
  const value = values.get(key); if (!value) throw new Error(`Missing ${key}`); return value;
}

interface RemoteFile<T> { value: T; sha: string | null }
function ghErrorStatus(error: unknown): number | null {
  const text = Buffer.concat([
    Buffer.isBuffer((error as { stdout?: unknown })?.stdout) ? (error as { stdout: Buffer }).stdout : Buffer.alloc(0),
    Buffer.isBuffer((error as { stderr?: unknown })?.stderr) ? (error as { stderr: Buffer }).stderr : Buffer.alloc(0),
  ]).toString('utf8');
  const match = /HTTP\s+(\d{3})/i.exec(text);
  return match ? Number(match[1]) : null;
}
async function ghJson(args: string[], signal?: AbortSignal): Promise<JsonObject> {
  const { stdout } = await runFile('gh', args, { signal }); return JSON.parse(stdout.toString('utf8')) as JsonObject;
}
export async function ensureStateBranch(repository: string, request: (args: string[]) => Promise<JsonObject> = ghJson): Promise<void> {
  const repo = await request(['api', `/repos/${repository}`]);
  if (repo.private !== true || typeof repo.default_branch !== 'string') throw new Error('Study requires a private repository with a default branch');
  try {
    await request(['api', `/repos/${repository}/git/ref/heads/${STUDY_STATE_BRANCH}`]);
    return;
  } catch (error) {
    if (ghErrorStatus(error) !== 404) throw error;
  }
  const base = await request(['api', `/repos/${repository}/git/ref/heads/${repo.default_branch}`]);
  const sha = (base.object as JsonObject | undefined)?.sha;
  if (typeof sha !== 'string') throw new Error('Default branch reference is invalid');
  try {
    await request(['api', '--method', 'POST', `/repos/${repository}/git/refs`,
      '-f', `ref=refs/heads/${STUDY_STATE_BRANCH}`, '-f', `sha=${sha}`]);
  } catch (error) {
    if (ghErrorStatus(error) !== 422) throw error;
    await request(['api', `/repos/${repository}/git/ref/heads/${STUDY_STATE_BRANCH}`]);
  }
}
async function readRemoteFile<T>(repository: string, file: string): Promise<RemoteFile<T>> {
  try {
    const response = await ghJson(['api', `/repos/${repository}/contents/${file}?ref=observation-state`]);
    if (typeof response.content !== 'string' || typeof response.sha !== 'string') throw new Error('Invalid GitHub state response');
    return { value: JSON.parse(Buffer.from(response.content.replaceAll('\n', ''), 'base64').toString('utf8')) as T, sha: response.sha };
  } catch (error) {
    if (ghErrorStatus(error) === 404 && file === STATE_PATH) return { value: createStudyLedger() as T, sha: null };
    throw error;
  }
}
async function readRemoteText(repository: string, file: string): Promise<RemoteFile<string>> {
  try {
    const response = await ghJson(['api', `/repos/${repository}/contents/${file}?ref=observation-state`]);
    if (typeof response.content !== 'string' || typeof response.sha !== 'string') throw new Error('Invalid GitHub text response');
    return { value: Buffer.from(response.content.replaceAll('\n', ''), 'base64').toString('utf8'), sha: response.sha };
  } catch (error) {
    if (ghErrorStatus(error) === 404) return { value: '', sha: null };
    throw error;
  }
}
async function putRemoteFile(repository: string, file: string, content: string, sha: string | null, message: string): Promise<void> {
  const payload = path.join(tmpdir(), `market-study-put-${randomUUID()}.json`);
  atomicJson(payload, { message, content: Buffer.from(content).toString('base64'), branch: 'observation-state', ...(sha ? { sha } : {}) });
  try { await ghJson(['api', '--method', 'PUT', `/repos/${repository}/contents/${file}`, '--input', payload]); }
  finally { unlinkSync(payload); }
}
export function ledgerReadme(ledger: StudyLedger, repository: string): string {
  const development = new Set(ledger.days.filter(day => day.phase === 'DEVELOPMENT' && day.quality.status === 'PASS').map(day => day.sessionDate));
  const holdout = new Set(ledger.days.filter(day => day.phase === 'HOLDOUT' && day.quality.status === 'PASS').map(day => day.sessionDate));
  const rejected = ledger.days.filter(day => day.quality.status === 'INSUFFICIENT_DATA').length;
  const aggregates = new Map<string, { phase: string; scenario: string; days: number; entries: number; fees: number;
    unresolved: number; net: number; netKnown: boolean }>();
  for (const day of ledger.days.filter(item => item.quality.status === 'PASS')) for (const raw of day.results) {
    if (!raw || typeof raw !== 'object') continue;
    const result = raw as Record<string, unknown>, scenario = String(result.name ?? 'unknown');
    const key = `${day.phase}:${scenario}`, item = aggregates.get(key) ?? { phase: day.phase, scenario,
      days: 0, entries: 0, fees: 0, unresolved: 0, net: 0, netKnown: true };
    item.days += 1; item.entries += Number(result.entries ?? 0); item.fees += Number(result.feesRub ?? 0);
    item.unresolved += Number(result.unresolvedPositions ?? 0);
    if (typeof result.netPnlRub === 'number' && Number.isFinite(result.netPnlRub)) item.net += result.netPnlRub;
    else item.netKnown = false;
    aggregates.set(key, item);
  }
  const rows = [...aggregates.values()].map(item => `| ${item.phase} | ${item.scenario} | ${item.days} | ${item.entries} | ${item.netKnown ? item.net.toFixed(2) : '—'} | ${item.fees.toFixed(2)} | ${item.unresolved} |`).join('\n');
  const table = rows ? `\n| Phase | Scenario | Days | Entries | Net PnL, RUB | Fees, RUB | Unresolved |\n| --- | --- | ---: | ---: | ---: | ---: | ---: |\n${rows}\n` : '';
  return `# Market study state\n\n- Phase: **${ledger.phase}**\n- Protocol: \`${ledger.protocolHash}\`\n- Development days: ${development.size}/10\n- Holdout days: ${holdout.size}/10\n- Rejected finalized days: ${rejected}\n- Attempts: ${ledger.attempts.length}/60\n- Confirmed immutable chunks: ${ledger.chunks.length}\n- Archive: [private draft release](https://github.com/${repository}/releases/tag/${STUDY_RELEASE_TAG})\n- Updated: ${ledger.updatedAt}\n${table}\nQuality acceptance is independent of replay PnL. The table includes passing days only, is descriptive, and never selects a winner. Raw recordings are release assets, never Git blobs.\n`;
}
export async function updateRemoteLedger(
  repository: string,
  mutate: (ledger: StudyLedger) => StudyLedger,
  message: string,
): Promise<StudyLedger> {
  await ensureStateBranch(repository);
  let desired: StudyLedger | null = null;
  for (let retry = 0; retry < 5; retry += 1) {
    const remote = await readRemoteFile<StudyLedger>(repository, STATE_PATH);
    desired = desired ? mergeStudyLedgers(remote.value, desired) : mutate(remote.value);
    try {
      await putRemoteFile(repository, STATE_PATH, `${JSON.stringify(desired, null, 2)}\n`, remote.sha, message);
      try {
        const readme = await readRemoteText(repository, README_PATH);
        await putRemoteFile(repository, README_PATH, ledgerReadme(desired, repository), readme.sha, 'Update market study status');
      } catch { /* README is a derived status view; the ledger commit is authoritative. */ }
      return desired;
    } catch (error) {
      if (![409, 422].includes(ghErrorStatus(error) ?? 0)) throw error;
      if (retry === 4) throw new Error('Study ledger optimistic update failed after retries');
    }
  }
  throw new Error('Study ledger update failed');
}

function dayStartUtc(date: string): number {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date); if (!match) throw new Error('Invalid study date');
  return Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])) - 3 * 3_600_000;
}
export function sandboxDiscoveryFailure(error: unknown) {
  const candidate = (error as { code?: unknown } | null)?.code;
  const grpcCode = typeof candidate === 'number' && Number.isInteger(candidate) && candidate >= 0 && candidate <= 16 ? candidate : null;
  const names: Record<number, string> = { 4: 'DEADLINE_EXCEEDED', 7: 'PERMISSION_DENIED', 8: 'RESOURCE_EXHAUSTED',
    14: 'UNAVAILABLE', 16: 'UNAUTHENTICATED' };
  const raw = error as { details?: unknown; message?: unknown } | null;
  const details = typeof raw?.details === 'string' ? raw.details : typeof raw?.message === 'string' ? raw.message : '';
  const httpStatus = /HTTP[^\r\n]{0,50}?\b(401|403|404|429|500|502|503)\b/i.exec(details)?.[1];
  const transport = httpStatus ? `HTTP_${httpStatus}` : /ENOTFOUND|EAI_AGAIN|name resolution/i.test(details) ? 'DNS'
    : /certificate|ERR_TLS|SSL|TLS handshake/i.test(details) ? 'TLS'
    : /RST_STREAM|stream reset/i.test(details) ? 'STREAM_RESET'
    : /ECONNREFUSED|ECONNRESET|connection.*(?:refused|closed|reset)|No connection established/i.test(details) ? 'CONNECTION'
    : /timeout|timed out/i.test(details) ? 'TIMEOUT' : 'UNCLASSIFIED';
  return { stage: 'sandbox-discovery', grpcCode, reason: grpcCode === null ? 'DISCOVERY_FAILED' : names[grpcCode] ?? 'RPC_FAILED', transport };
}
async function discoverStudyMarket(signal: AbortSignal) {
  const token = process.env.TINKOFF_API_TOKEN_SANDBOX?.trim();
  if (!token) throw new Error('TINKOFF_API_TOKEN_SANDBOX is required');
  const api = new TinkoffInvestApi({ token, endpoint: TINKOFF_SANDBOX_ENDPOINT });
  try {
    return await discoverMarketPilot(api, STUDY_TICKERS, Date.now(), signal);
  } catch (error) {
    console.error(JSON.stringify(sandboxDiscoveryFailure(error)));
    throw error;
  } finally {
    const closable = api as TinkoffInvestApi & { channel?: { close(): void } };
    closable.channel?.close();
  }
}
async function discoverBlockPlan(block: StudyBlock, signal: AbortSignal): Promise<StudyBlockPlan | null> {
  const discovery = await discoverStudyMarket(signal), now = Date.now();
  const moscowDate = new Date(now + 3 * 3_600_000).toISOString().slice(0, 10);
  const window = nextMainSessionWindow(discovery.instruments, discovery.intervals, dayStartUtc(moscowDate), 1);
  if (!window) return null;
  return planRecoverableStudyBlock(moscowDate, window.start, window.end, block, now);
}

/** Runs at any hour; proves sandbox authentication AND both private write paths.
 * It deliberately makes no claim about market stream quality. */
async function preflight(workspace: string, repository: string, signal: AbortSignal) {
  console.log('Preflight: checking sandbox instruments and main-session calendar.');
  const discovery = await discoverStudyMarket(signal);
  const nextSession = nextMainSessionWindow(discovery.instruments, discovery.intervals, Date.now(), 60_000);
  if (!nextSession) throw new Error('No upcoming common main session is available');
  signal.throwIfAborted();
  console.log('Preflight: verifying private ledger write and archive upload.');
  const ledger = await updateRemoteLedger(repository, current => current, 'Verify market study state storage');
  mkdirSync(path.resolve(workspace), { recursive: true });
  const directory = mkdtempSync(path.join(path.resolve(workspace), 'preflight-'));
  let archive: string | undefined;
  try {
    const receipt = { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, diagnosticOnly: true, counted: false,
      sandboxEndpoint: TINKOFF_SANDBOX_ENDPOINT, instrumentCount: discovery.instruments.length,
      nextSession, marketStreamChecked: false, phase: ledger.phase, checkedAt: new Date().toISOString() };
    atomicJson(path.join(directory, 'preflight.json'), receipt);
    archive = await archiveChunk(directory, `preflight-${randomUUID()}.tar.gz`);
    await uploadConfirmedAsset(repository, archive);
    return { action: 'PREFLIGHT_COMPLETE', ...receipt, privateArchiveConfirmed: true };
  } finally {
    rmSync(directory, { recursive: true, force: true });
    if (archive) rmSync(archive, { force: true });
  }
}

interface ChunkQuality {
  status: 'PASS' | 'INSUFFICIENT_DATA';
  checks: Record<string, boolean>;
}
export function assessStudyChunk(directory: string, bounds?: Pick<StudyChunkPlan, 'plannedStart' | 'plannedEnd'>): ChunkQuality {
  const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as JsonObject;
  const summary = JSON.parse(readFileSync(path.join(directory, 'summary.json'), 'utf8')) as JsonObject;
  const groups = Array.isArray(summary.groups) ? summary.groups as JsonObject[] : [];
  const instruments = Array.isArray(manifest.instruments) ? manifest.instruments as JsonObject[] : [];
  const sampling = summary.samplingCoverage as JsonObject | undefined;
  const first = Date.parse(String(summary.firstReceivedAt ?? ''));
  const last = Date.parse(String(summary.lastReceivedAt ?? ''));
  const checks = {
    complete: summary.complete === true && (manifest.capture as JsonObject | undefined)?.reason === 'duration',
    exchange: manifest.source === 'exchange' && Number(summary.exchangeSamples) > 0,
    subscriptions: summary.allSubscriptionsAcknowledged === true && Array.isArray(summary.expectedSubscriptions)
      && summary.expectedSubscriptions.length === 18,
    timerCoverage: Number(sampling?.recordedShare ?? 0) >= 0.99,
    plannedBounds: !bounds || (Number.isFinite(first) && Number.isFinite(last)
      && first >= Date.parse(bounds.plannedStart) && last <= Date.parse(bounds.plannedEnd)),
    instruments: instruments.length === 6 && instruments.every((instrument) => {
      const group = groups.find(item => item.source === 'EXCHANGE' && item.ticker === instrument.ticker);
      const phases = Array.isArray(group?.phases) ? group.phases as JsonObject[] : [];
      const phase = phases.find(item => item.phase === 'regular_trading_session_main');
      return Number(phase?.usableShareOfObservedScheduledTicks ?? 0) >= 0.8;
    }),
  };
  return { status: Object.values(checks).every(Boolean) ? 'PASS' : 'INSUFFICIENT_DATA', checks };
}

/** Only fixed labels, known tickers and validated counts may enter public diagnostics. */
export function smokeQualityReasons(summary: JsonObject, quality: ChunkQuality): string[] {
  if (quality.status === 'PASS') return [];
  const count = (value: unknown): number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : 0;
  const reasons: string[] = [];
  const labels: Record<string, string> = {
    complete: 'Запись не завершила полную минуту.', exchange: 'Не получены пригодные биржевые данные.',
    subscriptions: 'Подтверждены не все 18 подписок.', plannedBounds: 'Запись вышла за границы заданного окна.',
  };
  for (const [key, label] of Object.entries(labels)) if (quality.checks[key] === false) reasons.push(label);
  if (quality.checks.timerCoverage === false) {
    const sampling = summary.samplingCoverage as JsonObject | undefined;
    reasons.push(`Таймер: записано ${count(sampling?.recordedTicks)} из ${count(sampling?.expectedTicksFromElapsedTime)} отметок; требуется 99%.`);
  }
  if (quality.checks.instruments === false) {
    const groups = Array.isArray(summary.groups) ? summary.groups as JsonObject[] : [];
    for (const ticker of STUDY_TICKERS) {
      const group = groups.find(item => item?.source === 'EXCHANGE' && item.ticker === ticker);
      const phases = Array.isArray(group?.phases) ? group.phases as JsonObject[] : [];
      const phase = phases.find(item => item?.phase === 'regular_trading_session_main');
      const share = phase?.usableShareOfObservedScheduledTicks;
      if (typeof share === 'number' && Number.isFinite(share) && share >= 0.8) continue;
      const observed = count(phase?.observedScheduledTicks), usable = count(phase?.eligibleSamples);
      reasons.push(observed ? `${ticker}: пригодно ${usable} из ${observed} секунд, требуется минимум ${Math.ceil(observed * 0.8)} (80%).`
        : `${ticker}: нет пригодных данных основной сессии.`);
    }
  }
  return reasons.length ? reasons : ['Проверка качества данных не пройдена.'];
}

const smokeStageLabels = {
  recording: 'Не удалось завершить полную минуту записи. Проверьте доступ к песочнице и состояние подключения.',
  quality: 'Не удалось проверить качество сохранённой записи.',
  replay: 'Не удалось выполнить симуляцию по сохранённой записи.',
  'private-archive': 'Не удалось подтвердить сохранение архива в приватном хранилище.',
  unknown: 'Техническая ошибка стартовой проверки. Автоматические повторы остановлены.',
} as const;

export function renderSmokeCheckEvent(event: SmokeCheckEvent, privateArchive = true): string {
  switch (event.type) {
    case 'attempt': return `${event.attempt === 1 ? '### Проверка перед сбором\n\n' : ''}Проба **${event.attempt}/3**: запись одной минуты.`;
    case 'quality-rejected': return `Проба **${event.attempt}/3** не прошла качество.\n\n${event.reasons.map(reason => `- ${reason}`).join('\n')}\n\n${privateArchive ? 'Диагностический архив сохранён приватно.' : 'Диагностика сохранена локально.'}`;
    case 'retry': return `Следующая проба начнётся автоматически через ${event.delayMs / 1000} секунд. Пороги качества сохранены.`;
    case 'passed': return `**Стартовая проверка пройдена с попытки ${event.attempt}/3.** Качество записи и выполнение симуляции подтверждены${privateArchive ? ', приватный архив проверен' : ''}. Это подтверждение старта; длительный сбор ещё не завершён.`;
    case 'stopped': return `**Сбор не начат.** ${event.reason === 'QUALITY_RETRIES_EXHAUSTED' ? 'Все три пробы отклонены по качеству данных.' : 'До конца допустимого окна недостаточно времени для полной пробы.'} Требования к качеству не снижались.`;
    case 'fatal': return `**Сбор не начат.** ${smokeStageLabels[event.stage]} Эта ошибка не повторяется автоматически.`;
  }
}

function reportSmokeCheckEvent(event: SmokeCheckEvent, privateArchive: boolean): void {
  const message = renderSmokeCheckEvent(event, privateArchive);
  console.log(`Market study: ${message}`);
  if (process.env.GITHUB_STEP_SUMMARY) writeFileSync(process.env.GITHUB_STEP_SUMMARY, `${message}\n\n`, { flag: 'a' });
  if (event.type === 'stopped' || event.type === 'fatal') {
    console.error(`::error title=Стартовая проверка::${event.type === 'fatal' ? smokeStageLabels[event.stage]
      : event.reason === 'QUALITY_RETRIES_EXHAUSTED' ? 'Все три пробы не прошли качество. Сбор не начат; причины указаны в сводке запуска.'
        : 'Истекло допустимое время стартовой проверки. Сбор не начат.'}`);
  }
}

function checkedSmoke(workspace: string, repository: string | undefined, signal: AbortSignal, deadlineMs?: number): Promise<string> {
  return runSmokeWithRetries(deadline => smoke(workspace, repository, deadline), { signal, deadlineMs,
    onEvent: event => reportSmokeCheckEvent(event, Boolean(repository)) });
}

function recorderArguments(outputDir: string, seconds: number, session: 'main' | 'any'): RecorderArguments {
  return { seconds, tickers: [...STUDY_TICKERS], source: 'exchange', depth: 20, budgetRub: 4_000,
    commissionRate: 0.0005, outputDir, maxBytes: 256 * 1024 * 1024,
    segmentMaxBytes: 32 * 1024 * 1024, session };
}
async function ensurePrivateRepository(repository: string): Promise<void> {
  const repo = await ghJson(['api', `/repos/${repository}`]);
  if (repo.private !== true) throw new Error('Market-study raw data requires a private GitHub repository');
}
/** The by-tag endpoint excludes drafts. List authenticated releases instead. */
export async function findDraftRelease(repository: string,
  request: (args: string[]) => Promise<unknown> = ghJson): Promise<{ id: number } | null> {
  let found: { id: number } | null = null;
  for (let page = 1; page <= 10; page += 1) {
    const releases = await request(['api', `/repos/${repository}/releases?per_page=100&page=${page}`]);
    if (!Array.isArray(releases)) throw new Error('Invalid release inventory');
    for (const raw of releases) {
      if (!raw || typeof raw !== 'object' || raw.tag_name !== STUDY_RELEASE_TAG) continue;
      if (raw.draft !== true || !Number.isSafeInteger(raw.id) || raw.id <= 0) {
        throw new Error('Study archive release must be a valid private draft');
      }
      if (found) throw new Error('Multiple study archive drafts require reconciliation');
      found = { id: raw.id };
    }
    if (releases.length < 100) return found;
  }
  throw new Error('Repository release inventory exceeds the supported limit');
}
async function ensureDraftRelease(repository: string): Promise<void> {
  await ensurePrivateRepository(repository);
  if (await findDraftRelease(repository)) return;
  try {
    await runFile('gh', ['release', 'create', STUDY_RELEASE_TAG, '--repo', repository, '--draft',
      '--title', 'Market study immutable archive', '--notes', 'Private draft release for read-only market-study chunks.']);
  } catch (createError) {
    if (!await findDraftRelease(repository)) throw createError;
  }
  if (!await findDraftRelease(repository)) throw new Error('Created archive draft was not confirmed');
}
interface ReleaseAsset { id: number; name: string; size: number; digest?: string }
async function releaseAssets(repository: string, signal?: AbortSignal): Promise<ReleaseAsset[]> {
  const release = await findDraftRelease(repository);
  if (!release) throw new Error('Archive draft is missing from the release inventory');
  const result: ReleaseAsset[] = [];
  for (let page = 1; page <= 200; page += 1) {
    signal?.throwIfAborted();
    const response = await ghJson(['api', `/repos/${repository}/releases/${Number(release.id)}/assets?per_page=100&page=${page}`], signal);
    if (!Array.isArray(response)) throw new Error('Invalid release asset page');
    for (const raw of response as JsonObject[]) {
      const asset = { id: Number(raw.id), name: String(raw.name), size: Number(raw.size),
        digest: typeof raw.digest === 'string' ? raw.digest : undefined };
      if (!Number.isSafeInteger(asset.id) || asset.id <= 0 || !asset.name || !Number.isSafeInteger(asset.size) || asset.size <= 0) {
        throw new Error('Invalid release asset metadata');
      }
      result.push(asset);
    }
    if (response.length < 100) return result;
  }
  throw new Error('Draft release reached its supported 20000-asset inventory');
}
async function downloadAsset(repository: string, assetId: number, destination: string, signal?: AbortSignal): Promise<void> {
  const { stdout } = await runFile('gh', ['api', '-H', 'Accept: application/octet-stream',
    `/repos/${repository}/releases/assets/${assetId}`], { signal });
  writeFileSync(destination, stdout, { mode: 0o600 });
}
async function uploadConfirmedAsset(repository: string, archive: string, signal?: AbortSignal): Promise<ReleaseAsset> {
  signal?.throwIfAborted();
  await ensureDraftRelease(repository);
  return confirmImmutableAsset({ name: path.basename(archive), bytes: statSync(archive).size, sha256: sha256File(archive) }, {
    find: async () => (await releaseAssets(repository, signal)).find(item => item.name === path.basename(archive)),
    upload: async () => { await runFile('gh', ['release', 'upload', STUDY_RELEASE_TAG, archive, '--repo', repository], { signal }); },
    downloadedHash: async asset => {
      const downloaded = path.join(tmpdir(), `market-study-verify-${randomUUID()}.tar.gz`);
      try { await downloadAsset(repository, asset.id, downloaded, signal); return sha256File(downloaded); }
      finally { rmSync(downloaded, { force: true }); }
    },
  }, signal);
}

interface ChunkDraft extends Omit<StudyChunkReceipt, 'assetId' | 'assetDigest' | 'assetBytes' | 'archiveSha256'> {
  collection?: RecoveryCollectionMetadata;
  acquisitionPolicyHash?: string;
}
interface RecoveryCollectionMetadata {
  mode: 'RECOVERY'; requestedBlock: StudyBlock; selectedAt: string; partialStart: boolean;
  mainStart: string; mainEnd: string; ownedStart: string; ownedEnd: string;
}
/** Immutable provenance describes missing time without changing scientific acceptance. */
export function studyCollectionMetadata(plan: StudyBlockPlan): RecoveryCollectionMetadata | undefined {
  return plan.recovery ? { mode: 'RECOVERY', ...plan.recovery, mainStart: plan.mainStart,
    mainEnd: plan.mainEnd, ownedStart: plan.ownedStart, ownedEnd: plan.ownedEnd } : undefined;
}
interface DayDraft extends Omit<StudyDayReceipt,
  'reportAssetId' | 'reportAssetName' | 'reportAssetDigest' | 'reportAssetBytes' | 'reportArchiveSha256'> {}
function readManifestIdentity(directory: string): { runId: string; manifestHash: string; recordingHash: string } {
  const file = path.join(directory, 'manifest.json'), bytes = readFileSync(file);
  const manifest = JSON.parse(bytes.toString('utf8')) as JsonObject;
  const recording = manifest.recording as JsonObject | undefined;
  if (typeof manifest.runId !== 'string' || typeof recording?.sha256 !== 'string') throw new Error('Completed chunk identity is missing');
  return { runId: manifest.runId, manifestHash: createHash('sha256').update(bytes).digest('hex'), recordingHash: recording.sha256 };
}
async function archiveChunk(directory: string, name: string, companion?: string, signal?: AbortSignal): Promise<string> {
  const archive = path.join(path.dirname(directory), name);
  if (companion && path.dirname(path.resolve(companion)) !== path.dirname(path.resolve(directory))) {
    throw new Error('Archive companion must be beside the recording');
  }
  await runFile('tar', ['-czf', archive, '-C', path.dirname(directory), path.basename(directory),
    ...(companion ? [path.basename(companion)] : [])], { signal });
  return archive;
}
export async function waitUntil(when: number, signal: AbortSignal): Promise<void> {
  const delay = when - Date.now();
  if (delay <= 0) return;
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true; clearTimeout(timer); signal.removeEventListener('abort', stop);
      error ? reject(error) : resolve();
    };
    const timer = setTimeout(() => finish(), delay);
    const stop = () => finish(new Error('Study block aborted'));
    signal.addEventListener('abort', stop, { once: true });
    if (signal.aborted) stop();
  });
}

/** The probe resolves only after quality, replay and private upload all pass.
 * Keep the original chunk clock: probe time is a real gap in daily coverage. */
export async function runAfterBlockCheck<T>(notBefore: number, signal: AbortSignal,
  probe: () => Promise<unknown>, capture: () => Promise<T>): Promise<T> {
  signal.throwIfAborted();
  await waitUntil(notBefore, signal);
  signal.throwIfAborted();
  await probe();
  signal.throwIfAborted();
  return capture();
}

function sameHashes(left: Record<string, string>, right: Record<string, string>): boolean {
  return canonicalJson(left) === canonicalJson(right);
}
function assertFrozenRuntime(ledger: StudyLedger, root = process.cwd()): void {
  if (ledger.phase !== 'HOLDOUT') return;
  if (!ledger.freeze || ledger.freeze.replayConfigHash !== replayConfigHash()
    || !sameHashes(ledger.freeze.simulatorHashes, replaySourceHashes(root))) {
    throw new Error('Current replay implementation differs from the holdout freeze');
  }
}

function safeArchiveEntries(raw: string): string[] {
  const entries = raw.split(/\r?\n/).filter(Boolean);
  if (!entries.length || entries.some(entry => path.posix.isAbsolute(entry)
    || entry.split('/').some(part => part === '..') || entry.includes('\\'))) {
    throw new Error('Release archive has an unsafe path');
  }
  return entries;
}
async function inspectArchive(archive: string): Promise<{ entries: string[]; receiptEntry: string; kind: 'chunk' | 'day' }> {
  const listed = await runFile('tar', ['-tzf', archive], { maxBuffer: 16 * 1024 * 1024 });
  const entries = safeArchiveEntries(listed.stdout.toString('utf8'));
  const receipts = entries.filter(entry => /\/(study-chunk-receipt|study-day-receipt)\.json$/.test(entry));
  if (receipts.length !== 1) throw new Error('Release archive must contain one study receipt');
  return { entries, receiptEntry: receipts[0], kind: receipts[0].endsWith('/study-day-receipt.json') ? 'day' : 'chunk' };
}
function validatedRecoveredDay(raw: unknown, asset: ReleaseAsset, archiveSha256: string, ledger: StudyLedger): StudyDayReceipt {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('Day report receipt is invalid');
  const draft = raw as Record<string, unknown>;
  if (['reportAssetId', 'reportAssetDigest', 'reportAssetBytes', 'reportArchiveSha256'].some(key => key in draft)) {
    throw new Error('Day draft contains mutable remote identity');
  }
  const receipt = { ...draft, reportAssetId: asset.id, reportAssetName: asset.name,
    reportAssetDigest: asset.digest ?? `sha256:${archiveSha256}`, reportAssetBytes: asset.size,
    reportArchiveSha256: archiveSha256 } as unknown as StudyDayReceipt;
  acceptStudyDay(ledger, receipt);
  return receipt;
}
function validatedRecoveredReceipt(raw: unknown, asset: ReleaseAsset, archiveSha256: string, ledger: StudyLedger): StudyChunkReceipt {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('Release receipt is invalid');
  const draft = raw as Record<string, unknown>;
  if ('assetId' in draft || 'assetDigest' in draft || 'assetBytes' in draft || 'archiveSha256' in draft) {
    throw new Error('Draft receipt contains mutable remote identity');
  }
  const receipt = { ...draft, assetId: asset.id, assetName: asset.name,
    assetDigest: asset.digest ?? `sha256:${archiveSha256}`, assetBytes: asset.size,
    archiveSha256 } as unknown as StudyChunkReceipt;
  // This is validation as well as a recovery dry-run; only the returned ledger is discarded.
  acceptStudyChunk(ledger, receipt);
  return receipt;
}

/** Recover a confirmed upload whose previous job died before its optimistic ledger commit. */
export async function reconcileReleaseAssets(repository: string): Promise<StudyLedger> {
  await ensurePrivateRepository(repository);
  const ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
  const known = new Set([...ledger.chunks.map(chunk => chunk.assetId), ...ledger.days.map(day => day.reportAssetId)]);
  const candidates = (await releaseAssets(repository)).filter(asset => /^(?:study-\d{4}-\d{2}-\d{2}-(?:early|late)-|study-day-)/.test(asset.name) && !known.has(asset.id));
  let current = ledger;
  for (const asset of candidates.sort((a, b) => a.id - b.id)) {
    const temporary = mkdtempSync(path.join(tmpdir(), 'market-study-reconcile-'));
    const archive = path.join(temporary, asset.name);
    try {
      await downloadAsset(repository, asset.id, archive);
      if (statSync(archive).size !== asset.size) throw new Error('Downloaded release asset size differs from inventory');
      const archiveHash = sha256File(archive);
      if (asset.digest && asset.digest !== `sha256:${archiveHash}`) throw new Error('Downloaded release asset digest differs from inventory');
      const { receiptEntry, kind } = await inspectArchive(archive);
      const receiptBytes = await runFile('tar', ['-xOzf', archive, receiptEntry], { maxBuffer: 1024 * 1024 });
      const raw = JSON.parse(receiptBytes.stdout.toString('utf8'));
      if (kind === 'chunk') {
        const receipt = validatedRecoveredReceipt(raw, asset, archiveHash, current);
        current = await updateRemoteLedger(repository, state => acceptStudyChunk(state, receipt), `Recover ${receipt.chunkId}`);
      } else {
        const receipt = validatedRecoveredDay(raw, asset, archiveHash, current);
        current = await updateRemoteLedger(repository, state => acceptStudyDay(state, receipt), `Recover ${receipt.sessionDate} report`);
      }
    } finally { rmSync(temporary, { recursive: true, force: true }); }
  }
  return current;
}

export async function materializeAsset(repository: string, receipt: StudyChunkReceipt, workspace: string): Promise<string> {
  const asset = (await releaseAssets(repository)).find(item => item.id === receipt.assetId);
  if (!asset || asset.name !== receipt.assetName || asset.size !== receipt.assetBytes) throw new Error('Canonical release asset metadata changed');
  const archive = path.join(workspace, asset.name);
  await downloadAsset(repository, asset.id, archive);
  if (sha256File(archive) !== receipt.archiveSha256) throw new Error('Canonical release asset content changed');
  if (asset.digest && asset.digest !== receipt.assetDigest) throw new Error('Canonical release asset digest changed');
  const { entries, receiptEntry, kind } = await inspectArchive(archive);
  if (kind !== 'chunk') throw new Error('Canonical chunk asset contains a day report');
  const topLevels = new Set(entries.map(entry => entry.split('/')[0]).filter(Boolean));
  if (topLevels.size !== 1) throw new Error('Chunk archive has multiple roots');
  const root = [...topLevels][0];
  if (!receiptEntry.startsWith(`${root}/`)) throw new Error('Chunk receipt path is inconsistent');
  await runFile('tar', ['-xzf', archive, '-C', workspace]);
  const directory = realpathSync(path.join(workspace, root));
  if (!directory.startsWith(`${realpathSync(workspace)}${path.sep}`)) throw new Error('Chunk extraction escaped its workspace');
  let recordingDirectory = directory;
  const referencePath = path.join(directory, 'continuous-reference.json');
  if (existsSync(referencePath)) {
    const reference = JSON.parse(readFileSync(referencePath, 'utf8')) as ContinuousReference;
    if (reference.schemaVersion !== 1 || reference.kind !== 'CONTINUOUS_WINDOW'
      || !supportedContinuousPolicyHash(reference.acquisitionPolicyHash) || reference.runId !== receipt.runId
      || reference.manifestHash !== receipt.manifestHash || reference.recordingHash !== receipt.recordingHash) {
      throw new Error('Continuous window reference identity differs from receipt');
    }
    recordingDirectory = await extractContinuousAsset(repository, reference.source, workspace);
  }
  const identity = readManifestIdentity(recordingDirectory);
  if (identity.manifestHash !== receipt.manifestHash || identity.recordingHash !== receipt.recordingHash) {
    throw new Error('Extracted chunk identity differs from its immutable receipt');
  }
  return recordingDirectory;
}

export async function materializeStudyInputs(repository: string, receipts: StudyChunkReceipt[], workspace: string): Promise<string[]> {
  const directories: string[] = [];
  for (const receipt of receipts) {
    const directory = await materializeAsset(repository, receipt, workspace);
    if (!directories.includes(directory)) directories.push(directory);
  }
  return directories;
}

function expectedChunkIds(plan: StudyBlockPlan): string[] {
  const earlyAt = Date.parse(`${plan.sessionDate}T05:50:00.000Z`);
  const lateAt = Date.parse(`${plan.sessionDate}T10:50:00.000Z`);
  const early = planStudyBlock(plan.sessionDate, plan.mainStart, plan.mainEnd, 'early', earlyAt);
  const late = planStudyBlock(plan.sessionDate, plan.mainStart, plan.mainEnd, 'late', lateAt);
  if (!early || !late) throw new Error('Full study day does not contain both configured blocks');
  return [...early.chunks.map(chunk => `${plan.sessionDate}:early:${chunk.index}`),
    ...late.chunks.map(chunk => `${plan.sessionDate}:late:${chunk.index}`)];
}

/** Finalize only an exact, immutable canonical set for the complete API session. */
export async function maybeFinalizeStudyDay(repository: string, plan: StudyBlockPlan, workspace: string): Promise<string | null> {
  const ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
  assertFrozenRuntime(ledger);
  const chunkIds = expectedChunkIds(plan), receipts: StudyChunkReceipt[] = [];
  for (const chunkId of chunkIds) {
    const assetId = ledger.canonicalChunks[chunkId];
    const receipt = ledger.chunks.find(chunk => chunk.assetId === assetId);
    if (!receipt) return null;
    receipts.push(receipt);
  }
  const phases = new Set(receipts.map(receipt => receipt.phase));
  if (phases.size !== 1) throw new Error('Canonical study day spans multiple phases');
  const phase = receipts[0].phase;
  const finalized = ledger.days.find(day => day.sessionDate === plan.sessionDate && day.phase === phase);
  if (finalized) return null;
  const inputHash = hashStudyValue(receipts.map(receipt => ({ chunkId: receipt.chunkId, assetId: receipt.assetId,
    archiveSha256: receipt.archiveSha256, recordingHash: receipt.recordingHash })));
  const materialized = mkdtempSync(path.join(path.resolve(workspace), `day-${plan.sessionDate}-`));
  const outputDirectory = path.join(materialized, 'replay');
  try {
    const directories = await materializeStudyInputs(repository, receipts, materialized);
    const report = await replaySession(directories, outputDirectory, { sessionDate: plan.sessionDate,
      mainStart: plan.mainStart, mainEnd: plan.mainEnd });
    const sourceHashes = replaySourceHashes(process.cwd());
    if (report.configHash !== replayConfigHash() || !sameHashes(report.simulatorHashes, sourceHashes)) {
      throw new Error('Replay report source identity differs from the running implementation');
    }
    const dayDraft: DayDraft = { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH,
      dayId: `${STUDY_PROTOCOL_HASH}:${plan.sessionDate}`, sessionDate: plan.sessionDate, phase,
      mainStart: plan.mainStart, mainEnd: plan.mainEnd, chunkAssetIds: receipts.map(receipt => receipt.assetId),
      canonicalInputHash: inputHash, replayConfigHash: report.configHash, simulatorHashes: report.simulatorHashes,
      quality: report.quality, results: report.results, finalizedAt: new Date().toISOString() };
    atomicJson(path.join(outputDirectory, 'study-day-receipt.json'), dayDraft);
    const reportAssetName = `study-day-${plan.sessionDate}-${inputHash.slice(0, 16)}.tar.gz`;
    const reportArchive = await archiveChunk(outputDirectory, reportAssetName);
    const reportArchiveHash = sha256File(reportArchive);
    const reportAsset = await uploadConfirmedAsset(repository, reportArchive);
    const day: StudyDayReceipt = { ...dayDraft, reportAssetId: reportAsset.id, reportAssetName,
      reportAssetDigest: reportAsset.digest ?? `sha256:${reportArchiveHash}`,
      reportAssetBytes: reportAsset.size || statSync(reportArchive).size, reportArchiveSha256: reportArchiveHash };
    await updateRemoteLedger(repository, current => acceptStudyDay(current, day), `Finalize ${plan.sessionDate}`);
    const durableOutput = path.join(path.resolve(workspace), `replay-${plan.sessionDate}-${inputHash.slice(0, 12)}`);
    if (existsSync(durableOutput)) throw new Error('Study replay output collision');
    mkdirSync(durableOutput);
    for (const name of ['replay.json', 'fills.json', 'REPORT.md', 'study-day-receipt.json']) {
      writeFileSync(path.join(durableOutput, name), readFileSync(path.join(outputDirectory, name)));
    }
    return durableOutput;
  } finally { rmSync(materialized, { recursive: true, force: true }); }
}

export function studyBlockRecorded(ledger: StudyLedger, plan: StudyBlockPlan): boolean {
  return plan.chunks.length > 0 && plan.chunks.every(planned => ledger.chunks.some(recorded =>
    recorded.sessionDate === plan.sessionDate && recorded.block === plan.block
    && recorded.chunkIndex === planned.index && recorded.plannedStart === planned.plannedStart
    && recorded.plannedEnd === planned.plannedEnd
    && ledger.canonicalChunks[`${plan.sessionDate}:${plan.block}:${planned.index}`] === recorded.assetId));
}

export function studyChunkRecorded(ledger: StudyLedger, plan: StudyBlockPlan, chunk: StudyChunkPlan): boolean {
  return studyBlockRecorded(ledger, { ...plan, chunks: [chunk] });
}

/** Skip elapsed and confirmed windows while preserving every original chunk identity. */
export function remainingStudyChunks(plan: StudyBlockPlan, ledger: StudyLedger, nowMs: number): StudyChunkPlan[] {
  return plan.chunks.filter(chunk => chunkDurationSeconds(chunk, nowMs) > 0 && !studyChunkRecorded(ledger, plan, chunk));
}

/** Recovery has a fresh bounded startup budget and leaves time for actual capture. */
export function studyStartupDeadline(plan: StudyBlockPlan, nowMs: number): number {
  if (!Number.isFinite(nowMs)) throw new Error('Invalid startup clock');
  return plan.recovery
    ? Math.min(nowMs + 8 * 60_000, Date.parse(plan.ownedEnd) - 20_000)
    : Math.min(Date.parse(plan.ownedStart) + STUDY_LAUNCH_LATENESS_MS, Date.parse(plan.ownedEnd));
}

type BlockResult = { action: 'CAPTURED'; plan: StudyBlockPlan; chunks: number; reportPath: string | null;
    rawBlockConfirmed?: boolean; scientificProcessing?: 'DEFERRED_TO_REPORT' }
  | { action: 'SKIPPED'; reason: string; chunks: 0; reportPath: string | null };
export function studyRecorderArguments(workspace: string, chunk: StudyChunkPlan, nowMs: number, signal: AbortSignal): RecorderArguments {
  const seconds = chunkDurationSeconds(chunk, nowMs);
  if (seconds <= 0) throw new Error('Owned recording window expired');
  return { ...recorderArguments(workspace, seconds, 'main'),
    captureDeadlineMs: Date.parse(chunk.plannedEnd) - STUDY_CHUNK_END_MARGIN_MS,
    parentSignal: signal, setExitCodeOnFailure: false };
}
function appendStudySummary(message: string): void {
  console.log(message);
  if (process.env.GITHUB_STEP_SUMMARY) writeFileSync(process.env.GITHUB_STEP_SUMMARY, `${message}\n\n`, { flag: 'a' });
}
async function saveOperation(repository: string, operation: BlockOperation): Promise<void> {
  if (!/^\d+:\d+$/.test(operation.attemptId)) throw new Error('Invalid operational attempt identity');
  const file = `operations/${operation.attemptId.replace(':', '-')}.json`;
  operation.updatedAt = new Date().toISOString();
  for (let retry = 0; retry < 5; retry += 1) {
    const remote = await readRemoteText(repository, file);
    const desired = remote.value ? mergeBlockOperations(JSON.parse(remote.value) as BlockOperation, operation) : structuredClone(operation);
    try {
      await putRemoteFile(repository, file, `${JSON.stringify(desired, null, 2)}\n`, remote.sha, 'Update capture operational evidence');
      Object.assign(operation, desired); return;
    } catch (error) {
      if (![409, 422].includes(ghErrorStatus(error) ?? 0)) throw error;
      if (retry === 4) throw new Error('Operation optimistic update failed after retries');
    }
  }
}

/** Runs without broker access, including after a crashed job or on the next morning. */
export async function refreshOperationalReports(repository: string): Promise<number> {
  let ledger = await reconcileReleaseAssets(repository);
  let operations: BlockOperation[] = [];
  for (const attempt of ledger.attempts) {
    if (!/^\d+:\d+$/.test(attempt.attemptId)) continue;
    const text = await readRemoteText(repository, `operations/${attempt.attemptId.replace(':', '-')}.json`);
    if (text.value) operations.push(JSON.parse(text.value) as BlockOperation);
  }
  let replayFailures = 0;
  const recoveryWorkspace = mkdtempSync(path.join(tmpdir(), 'market-study-report-'));
  try {
    replayFailures += await reconcileContinuousBlocks(repository, recoveryWorkspace);
    ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
    for (const plan of closedPlansNeedingFinalization(ledger, operations)) {
      try { await maybeFinalizeStudyDay(repository, plan, recoveryWorkspace); }
      catch {
        replayFailures += 1;
        appendStudySummary('Полный научный отчёт не восстановлен: требуется проверка сохранённых данных и версии обработки. Операционная сводка будет сохранена отдельно.');
      }
    }
  } finally { rmSync(recoveryWorkspace, { recursive: true, force: true }); }
  ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
  const today = new Date(Date.now() + 3 * 3_600_000).toISOString().slice(0, 10);
  // Historical replay can take time; refresh today's operations before checking liveness.
  operations = operations.filter(operation => operation.plan.sessionDate !== today);
  for (const attempt of ledger.attempts.filter(attempt => attempt.sessionDate === today)) {
    if (!/^\d+:\d+$/.test(attempt.attemptId)) continue;
    const text = await readRemoteText(repository, `operations/${attempt.attemptId.replace(':', '-')}.json`);
    if (text.value) operations.push(JSON.parse(text.value) as BlockOperation);
  }
  const previous = new Date(`${today}T00:00:00Z`);
  do { previous.setUTCDate(previous.getUTCDate() - 1); } while ([0, 6].includes(previous.getUTCDay()));
  const dates = [...new Set([...ledger.attempts.map(item => item.sessionDate).filter((date): date is string => Boolean(date)), previous.toISOString().slice(0, 10), today])].sort();
  const runtime = await readStudyRuntime(process.env);
  for (const date of dates) {
    const day = operationalDay(ledger, operations, date, Date.now(), runtime);
    const file = `reports/daily-${date}.json`, previous = await readRemoteText(repository, file);
    await putRemoteFile(repository, file, `${JSON.stringify(day, null, 2)}\n`, previous.sha, `Report capture completeness ${date}`);
    appendStudySummary(renderOperationalDay(day));
  }
  const readme = await readRemoteText(repository, README_PATH);
  await putRemoteFile(repository, README_PATH, ledgerReadme(ledger, repository)
    + `\nOperational daily reports and separate DEVELOPMENT research summaries: [reports](https://github.com/${repository}/tree/${STUDY_STATE_BRANCH}/reports). Research results are linked by research-YYYY-MM-DD.json; incomplete days are never accepted as scientific evidence.\n`, readme.sha, 'Refresh study completeness status');
  if (replayFailures) throw new Error('Scientific report recovery failed; operational evidence was saved');
  if (!runtime.available) throw new Error('Collector activity could not be verified; operational evidence was saved with UNKNOWN status');
  return dates.length;
}

/** Derived diagnostics use existing fixed replay only. They never write the study ledger. */
export async function publishDailyResearchReport(
  summary: ReturnType<typeof buildDailyResearchReport>['json'],
  store: {
    read(file: string): Promise<RemoteFile<string>>;
    write(file: string, content: string, sha: string | null, message: string): Promise<void>;
  },
): Promise<void> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(summary.sessionDate) || !/^[0-9a-f]{64}$/.test(summary.identity)
    || summary.diagnosticOnly !== true || summary.counted !== false || summary.formalHoldout !== false) {
    throw new Error('Invalid private diagnostic report');
  }
  const base = `reports/research-${summary.sessionDate}-${summary.identity}`;
  const files = [[`${base}.json`, `${JSON.stringify(summary, null, 2)}\n`],
    [`${base}.md`, renderDailyResearchReport(summary)]];
  for (const [file, content] of files) {
    const previous = await store.read(file);
    if (previous.value && previous.value !== content) throw new Error('Immutable research content mismatch');
    if (!previous.value) await store.write(file, content, null, `Save research ${summary.sessionDate}`);
  }
  // Publish the mutable pointer last: a partial write cannot advertise a missing report.
  const pointer = `reports/research-${summary.sessionDate}.json`;
  const previous = await store.read(pointer);
  const content = `${JSON.stringify({ schemaVersion: 1, sessionDate: summary.sessionDate, state: 'READY',
    identity: summary.identity, diagnosticOnly: true, counted: false, json: `${base}.json`, markdown: `${base}.md` }, null, 2)}\n`;
  if (previous.value !== content) await store.write(pointer, content, previous.sha, `Index research ${summary.sessionDate}`);
}

/** Legacy archives predate operations files; their saved API calendar remains authoritative. */
export function recordedResearchPlan(sessionDate: string,
  instruments: Parameters<typeof nextMainSessionWindow>[0], intervals: Parameters<typeof nextMainSessionWindow>[1]): StudyBlockPlan {
  const window = nextMainSessionWindow(instruments, intervals, dayStartUtc(sessionDate), 1);
  if (!window || new Date(Date.parse(window.start) + 3 * 3_600_000).toISOString().slice(0, 10) !== sessionDate) {
    throw new Error('Archive does not confirm its research session date');
  }
  const plan = planStudyBlock(sessionDate, window.start, window.end, 'early', Date.parse(`${sessionDate}T05:50:00.000Z`));
  if (!plan) throw new Error('Archived research calendar cannot form the original full-day plan');
  return plan;
}

export async function refreshDailyResearchReports(repository: string): Promise<number> {
  await ensureStateBranch(repository);
  const ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
  const plans = new Map<string, StudyBlockPlan>();
  for (const attempt of ledger.attempts) {
    if (attempt.phase !== 'DEVELOPMENT' || !/^\d+:\d+$/.test(attempt.attemptId)) continue;
    const remote = await readRemoteText(repository, `operations/${attempt.attemptId.replace(':', '-')}.json`);
    if (!remote.value) continue;
    const operation = JSON.parse(remote.value) as BlockOperation;
    const plan = operation.plan;
    if (plan.sessionDate !== attempt.sessionDate) throw new Error('Research operation date mismatch');
    const previous = plans.get(plan.sessionDate);
    if (previous && (previous.mainStart !== plan.mainStart || previous.mainEnd !== plan.mainEnd)) {
      throw new Error('Conflicting research session calendars');
    }
    plans.set(plan.sessionDate, plan);
  }
  for (const date of new Set(ledger.attempts.filter(attempt => attempt.phase === 'DEVELOPMENT')
    .map(attempt => attempt.sessionDate).filter((date): date is string => Boolean(date)))) {
    if (plans.has(date)) continue;
    const first = ledger.chunks.filter(chunk => chunk.sessionDate === date && chunk.phase === 'DEVELOPMENT')
      .sort((a, b) => a.uploadedAt.localeCompare(b.uploadedAt) || a.assetId - b.assetId)[0];
    if (!first) continue;
    const temporary = mkdtempSync(path.join(tmpdir(), 'research-calendar-'));
    try {
      const directory = await materializeAsset(repository, first, temporary);
      const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as ObservationManifest;
      plans.set(date, recordedResearchPlan(date, manifest.instruments, manifest.intervals));
    } finally { rmSync(temporary, { recursive: true, force: true }); }
  }
  const identity = { replayConfigHash: replayConfigHash(), simulatorHashes: replaySourceHashes(),
    fixedScenarios: fixedReplayScenarios(), reportVersionHash: hashStudyValue({
      report: sha256File(path.resolve('src/report/daily-research.ts')),
      integration: sha256File(path.resolve('src/report/market-study.ts')),
    }) };
  let completed = 0, failures = 0;
  for (const plan of [...plans.values()].sort((a, b) => b.sessionDate.localeCompare(a.sessionDate))) {
    const selection = selectDailyResearchInputs(ledger, plan);
    if (selection.action !== 'closed') continue;
    const key = dailyResearchIdentity(selection, identity);
    const base = `reports/research-${plan.sessionDate}-${key}`;
    const pointer = `reports/research-${plan.sessionDate}.json`;
    const previousIndex = await readRemoteText(repository, pointer);
    if (previousIndex.value) {
      const index = JSON.parse(previousIndex.value) as { identity?: string; state?: string; reason?: string };
      if (index.identity === key && index.state === 'BLOCKED' && index.reason === 'INCOMPATIBLE_RECORDINGS') {
        appendStudySummary(`Диагностический анализ ${plan.sessionDate} ожидает совместимых данных; причина сохранена приватно.`);
        continue;
      }
    }
    const cached = await readRemoteText(repository, `${base}.json`);
    let summary: ReturnType<typeof buildDailyResearchReport>['json'];
    if (cached.value) {
      summary = JSON.parse(cached.value) as typeof summary;
      if (summary.identity !== key || summary.sessionDate !== plan.sessionDate || summary.diagnosticOnly !== true) {
        throw new Error('Cached research identity mismatch');
      }
    } else {
      const workspace = mkdtempSync(path.join(tmpdir(), `market-study-research-${plan.sessionDate}-`));
      try {
        const directories = await materializeStudyInputs(repository, selection.receipts, workspace);
        const replay = await replaySession(directories, path.join(workspace, 'replay'), {
          sessionDate: plan.sessionDate, mainStart: plan.mainStart, mainEnd: plan.mainEnd,
        });
        summary = buildDailyResearchReport(selection, replay, identity).json;
      } catch (error) {
        const reason = error instanceof Error && error.message === 'Incompatible chunk metadata or recorder version'
          ? 'INCOMPATIBLE_RECORDINGS' : 'RESEARCH_PROCESSING_FAILED';
        const previous = await readRemoteText(repository, pointer);
        await putRemoteFile(repository, pointer, `${JSON.stringify({ schemaVersion: 1, sessionDate: plan.sessionDate,
          state: 'BLOCKED', reason, identity: key, diagnosticOnly: true, counted: false,
          updatedAt: new Date().toISOString() }, null, 2)}\n`, previous.sha, `Report blocked research ${plan.sessionDate}`);
        appendStudySummary(`Диагностический анализ ${plan.sessionDate}: ${reason}. Подробности доступны только в приватном хранилище.`);
        // Mixed historical recorder versions are an explicit data boundary, not a runtime failure.
        if (reason !== 'INCOMPATIBLE_RECORDINGS') failures += 1;
        continue;
      } finally { rmSync(workspace, { recursive: true, force: true }); }
    }
    await publishDailyResearchReport(summary, {
      read: file => readRemoteText(repository, file),
      write: (file, content, sha, message) => putRemoteFile(repository, file, content, sha, message),
    });
    appendStudySummary(`Закреплённый диагностический анализ ${plan.sessionDate} сохранён приватно. Полнота дня и научный зачёт учитываются отдельно.`);
    completed += 1;
  }
  if (failures) throw new Error('Daily research failed; private diagnostic status saved');
  return completed;
}

export async function captureStudyBlock(repository: string, block: StudyBlock, workspace: string,
  runId: string, runAttempt: number, signal: AbortSignal): Promise<BlockResult> {
  if (process.env.MARKET_STUDY_ENABLED !== 'true') throw new Error('Full-session campaign is paused pending storage configuration, sandbox smoke and activation');
  const discoveredPlan = await discoverBlockPlan(block, signal);
  if (!discoveredPlan) return { action: 'SKIPPED', reason: 'OUTSIDE_BLOCK_WINDOW', chunks: 0, reportPath: null };
  mkdirSync(path.resolve(workspace), { recursive: true });
  await ensureDraftRelease(repository);
  const recovered = await reconcileReleaseAssets(repository);
  // Private reconciliation can cross the 14:00 split; select using the fresh clock.
  const plan = planRecoverableStudyBlock(discoveredPlan.sessionDate, discoveredPlan.mainStart, discoveredPlan.mainEnd, block, Date.now());
  if (!plan) return { action: 'SKIPPED', reason: 'OUTSIDE_BLOCK_WINDOW', chunks: 0, reportPath: null };
  block = plan.block;
  const decision = planStudyRun(recovered, { event: 'schedule', campaignEnabled: true, runId, runAttempt,
    sessionDate: plan.sessionDate });
  if (decision.action !== 'CAPTURE') return { action: 'SKIPPED', reason: decision.action, chunks: 0, reportPath: null };
  if (studyBlockRecorded(recovered, plan)) return { action: 'SKIPPED', reason: 'BLOCK_ALREADY_RECORDED', chunks: 0,
    reportPath: await maybeFinalizeStudyDay(repository, plan, workspace) };
  assertFrozenRuntime(recovered);
  if (!remainingStudyChunks(plan, recovered, Date.now()).length) return { action: 'SKIPPED', reason: 'NO_REMAINING_WINDOWS', chunks: 0, reportPath: null };
  if (plan.recovery) appendStudySummary(`Восстановление сбора: блок **${block}**; пропущенное время не восстанавливается, полный день ещё не подтверждён.`);
  const attemptId = `${runId}:${runAttempt}`;
  let persisted = await updateRemoteLedger(repository, ledger => beginStudyAttempt(ledger, { runId, runAttempt,
    sessionDate: plan.sessionDate, block, mode: 'COUNTED', startedAt: new Date().toISOString() }), `Begin ${plan.sessionDate} ${block}`);
  assertFrozenRuntime(persisted);
  const operation: BlockOperation = { schemaVersion: 1, attemptId, plan, startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(), state: 'STARTING', failure: null, parts: [] };
  await saveOperation(repository, operation);
  let stage: BlockOperation['failure'] = 'STARTUP';
  try {
    const result = await runAfterBlockCheck(Date.parse(plan.captureNotBefore), signal, async () => {
      appendStudySummary('Стартовая проверка: запись 60 секунд основной сессии.');
      await checkedSmoke(workspace, repository, signal, studyStartupDeadline(plan, Date.now()));
    }, async () => {
      operation.state = 'CAPTURING'; await saveOperation(repository, operation);
      let uploaded = 0;
      for (const chunk of remainingStudyChunks(plan, persisted, Date.now())) {
        signal.throwIfAborted();
        await waitUntil(Date.parse(chunk.plannedStart), signal);
        if (chunkDurationSeconds(chunk, Date.now()) <= 0) continue;
        stage = 'STORAGE_OR_PROCESSING';
        persisted = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
        assertFrozenRuntime(persisted);
        if (studyChunkRecorded(persisted, plan, chunk)) continue;
        operation.currentChunkIndex = chunk.index;
        await saveOperation(repository, operation);
        const directory = await recordOwnedSlot({ deadlineMs: Date.parse(chunk.plannedEnd) - STUDY_CHUNK_END_MARGIN_MS, signal,
          wait: (delay, parent) => waitUntil(Date.now() + delay, parent),
          attempt: async () => {
            stage = 'RECORDING';
            const directory = await recordMarket(studyRecorderArguments(workspace, chunk, Date.now(), signal), process.cwd());
            const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as JsonObject;
            if (manifest.status !== 'COMPLETE') {
              stage = 'STORAGE_OR_PROCESSING';
              const archive = await archiveChunk(directory, `failure-${runId}-${runAttempt}-${chunk.index}-${randomUUID()}.tar.gz`);
              const asset = await uploadConfirmedAsset(repository, archive);
              operation.parts.push({ index: chunk.index, status: 'RECORDING_FAILED', assetId: asset.id, quality: null,
                reasons: [recoverableRecordingFailure(manifest, Date.parse(chunk.plannedEnd) - STUDY_CHUNK_END_MARGIN_MS) ? 'TEMPORARY_METADATA_FAILURE' : 'TERMINAL_RECORDING_FAILURE'] });
              await saveOperation(repository, operation);
              appendStudySummary(`Часть ${chunk.index}: запись не завершена; диагностика сохранена приватно.`);
              if (!recoverableRecordingFailure(manifest, Date.parse(chunk.plannedEnd) - STUDY_CHUNK_END_MARGIN_MS)) { stage = 'RECORDING'; throw new Error('Terminal recording failure'); }
              return { value: directory, retryable: true };
            }
            return { value: directory, retryable: false };
          } });
        const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as JsonObject;
        if (manifest.status !== 'COMPLETE') continue;
        signal.throwIfAborted(); stage = 'STORAGE_OR_PROCESSING';
        const quality = assessStudyChunk(directory, chunk), identity = readManifestIdentity(directory);
        const assetName = `study-${plan.sessionDate}-${block}-${String(chunk.index).padStart(2, '0')}-${runId}-${runAttempt}.tar.gz`;
        const draft: ChunkDraft = { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, assetName,
          releaseTag: STUDY_RELEASE_TAG, attemptId, chunkId: `${plan.sessionDate}:${block}:${chunk.index}`,
          sessionDate: plan.sessionDate, phase: 'DEVELOPMENT', block, chunkIndex: chunk.index,
          plannedStart: chunk.plannedStart, plannedEnd: chunk.plannedEnd, runId: identity.runId,
          manifestHash: identity.manifestHash, recordingHash: identity.recordingHash,
          replayConfigHash: replayConfigHash(), simulatorHashes: replaySourceHashes(process.cwd()), quality: quality.status,
          uploadedAt: new Date().toISOString(),
          ...(plan.recovery ? { collection: studyCollectionMetadata(plan) } : {}) };
        const attempt = persisted.attempts.find(item => item.attemptId === attemptId);
        if (!attempt?.phase) throw new Error('Persisted study attempt disappeared');
        draft.phase = attempt.phase;
        atomicJson(path.join(directory, 'study-chunk-receipt.json'), draft);
        const archive = await archiveChunk(directory, assetName), archiveHash = sha256File(archive);
        const asset = await uploadConfirmedAsset(repository, archive);
        const receipt: StudyChunkReceipt = { ...draft, assetId: asset.id, assetDigest: asset.digest ?? `sha256:${archiveHash}`,
          assetBytes: asset.size || statSync(archive).size, archiveSha256: archiveHash };
        await updateRemoteLedger(repository, current => acceptStudyChunk(current, receipt), `Accept ${receipt.chunkId}`);
        const reasons = smokeQualityReasons(JSON.parse(readFileSync(path.join(directory, 'summary.json'), 'utf8')) as JsonObject, quality);
        operation.parts.push({ index: chunk.index, status: 'SAVED', assetId: asset.id, quality: quality.status, reasons });
        await saveOperation(repository, operation); uploaded += 1;
        appendStudySummary(`Часть ${chunk.index}: архив подтверждён; качество **${quality.status}**.`
          + (reasons.length ? `\n\n${reasons.map(reason => `- ${reason}`).join('\n')}` : ''));
      }
      const reportPath = await maybeFinalizeStudyDay(repository, plan, workspace);
      return { action: 'CAPTURED' as const, plan, chunks: uploaded, reportPath };
    });
    operation.state = 'FINISHED'; await saveOperation(repository, operation);
    return result;
  } catch (error) {
    operation.state = signal.aborted ? 'CANCELLED' : 'FAILED'; operation.failure = stage;
    try { await saveOperation(repository, operation); } catch { /* Preserve the primary failure; uploaded evidence is immutable. */ }
    appendStudySummary(`Сбор прерван. Этап: ${stage}. Ранее подтверждённые архивы сохранены; полный день не подтверждён.`);
    throw error;
  }
}

async function smoke(workspace: string, repository?: string, captureDeadlineMs?: number): Promise<string> {
  let directory: string, manifest: JsonObject;
  try {
    directory = await recordMarket({ ...recorderArguments(workspace, 60, 'main'), captureDeadlineMs }, process.cwd());
    manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as JsonObject;
  } catch { throw new StudyStageError('recording'); }
  const replayOutput = `${directory}-replay`;
  let quality: ChunkQuality | null = null, replay: Awaited<ReturnType<typeof replayRecording>> | null = null;
  let failure: SmokeQualityError | StudyStageError | null = null;
  try {
    if (manifest.status !== 'COMPLETE' || (manifest.settings as JsonObject | undefined)?.durationMs !== 60_000) {
      throw new StudyStageError('recording');
    }
    quality = assessStudyChunk(directory);
    if (quality.status !== 'PASS') throw new SmokeQualityError(smokeQualityReasons(
      JSON.parse(readFileSync(path.join(directory, 'summary.json'), 'utf8')) as JsonObject, quality));
    try { replay = await replayRecording(directory, replayOutput); }
    catch { throw new StudyStageError('replay'); }
  } catch (error) {
    failure = error instanceof SmokeQualityError || error instanceof StudyStageError ? error : new StudyStageError('quality');
  }
  try {
    atomicJson(path.join(directory, 'study-smoke.json'), { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH,
      diagnosticOnly: true, counted: false, status: manifest.status, quality,
      qualityReasons: failure instanceof SmokeQualityError ? failure.reasons : [],
      failureStage: failure instanceof StudyStageError ? failure.stage : failure ? 'quality' : null,
      replayStatus: replay ? 'COMPLETE' : failure instanceof StudyStageError && failure.stage === 'replay' ? 'FAILED' : 'NOT_RUN',
      replayQuality: replay?.quality ?? null, replayConfigHash: replay?.configHash ?? null, completedAt: new Date().toISOString() });
    if (repository) {
      if (typeof manifest.runId !== 'string' || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(manifest.runId)) {
        throw new Error('Invalid smoke recording identity');
      }
      await uploadConfirmedAsset(repository, await archiveChunk(directory, `smoke-${manifest.runId}.tar.gz`));
    }
  } catch { throw new StudyStageError('private-archive'); }
  if (failure) throw failure;
  return replayOutput;
}

export function diagnosticStopAt(nowMs: number, mainStart: string, mainEnd: string): number {
  const start = Date.parse(mainStart), end = Date.parse(mainEnd);
  if (![nowMs, start, end].every(Number.isFinite) || nowMs < start || nowMs >= end) throw new Error('Diagnostic capture requires an open main session');
  const date = new Date(nowMs + 3 * 3_600_000).toISOString().slice(0, 10);
  const stop = Math.min(Date.parse(`${date}T13:50:00+03:00`), end, nowMs + 340 * 60_000);
  if (stop - nowMs < 120_000) throw new Error('Diagnostic capture needs at least two minutes before the afternoon preparation');
  return stop;
}

/** Preserve raw input and sibling replay output together without writing derived files into the recording. */
export async function archiveDiagnosticRecording(directory: string, stopAt: number) {
  const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as JsonObject;
  const replayDirectory = `${directory}-replay`;
  let quality: ChunkQuality | null = null;
  let replayFailed = false;
  let processingStage = 'quality';
  if (manifest.status === 'COMPLETE') {
    try {
      quality = assessStudyChunk(directory);
      processingStage = 'replay';
      await replayRecording(directory, replayDirectory);
    } catch {
      replayFailed = true;
      console.error(JSON.stringify({ stage: `diagnostic-${processingStage}`, reason: 'PROCESSING_FAILED' }));
    }
  }
  const companion = existsSync(replayDirectory) ? replayDirectory : undefined;
  atomicJson(path.join(directory, 'diagnostic-observation.json'), { schemaVersion: 1, diagnosticOnly: true, counted: false,
    protocolHash: STUDY_PROTOCOL_HASH, status: manifest.status, quality, replayFailed, stopAt: new Date(stopAt).toISOString(),
    replayDirectory: companion ? path.basename(companion) : null,
    failureStage: replayFailed ? processingStage : null,
    replayScope: 'Each diagnostic part is replayed independently; not a full study day.' });
  const archive = await archiveChunk(directory, `diagnostic-${String(manifest.runId)}.tar.gz`, companion);
  return { archive, status: manifest.status, quality, replayFailed };
}

/** Collect the remaining morning independently of the full-day ledger. */
async function observeMorning(workspace: string, repository: string, signal: AbortSignal) {
  const discovery = await discoverStudyMarket(signal), now = Date.now();
  const window = nextMainSessionWindow(discovery.instruments, discovery.intervals, now, 60_000);
  if (!window) throw new Error('No open main session');
  const stopAt = diagnosticStopAt(now, window.start, window.end);
  console.log(JSON.stringify({ action: 'DIAGNOSTIC_STARTING', stopAt: new Date(stopAt).toISOString(), counted: false }));
  await checkedSmoke(workspace, repository, signal, stopAt);
  signal.throwIfAborted();
  console.log('Diagnostic startup passed: market quality, replay and private archive confirmed.');
  let uploaded = 0;
  while (stopAt - Date.now() >= 120_000) {
    signal.throwIfAborted();
    const seconds = Math.min(1_800, Math.floor((stopAt - Date.now() - 1_000) / 1_000));
    const directory = await recordMarket({ ...recorderArguments(workspace, seconds, 'main'),
      captureDeadlineMs: Math.min(stopAt, Date.now() + seconds * 1000), parentSignal: signal }, process.cwd());
    const result = await archiveDiagnosticRecording(directory, stopAt);
    await uploadConfirmedAsset(repository, result.archive);
    uploaded += 1;
    appendStudySummary(`Диагностическая часть ${uploaded}: запись, обработка и приватный архив завершены. Качество записи: ${result.quality?.status ?? 'FAILED'}. В зачёт дней не входит.`);
    console.log(JSON.stringify({ action: 'DIAGNOSTIC_PART_CONFIRMED', parts: uploaded, counted: false }));
    if (result.status !== 'COMPLETE') throw new Error('Diagnostic recording failed; partial archive preserved');
    if (result.replayFailed) throw new Error('Diagnostic replay failed; raw archive preserved');
  }
  return { action: 'DIAGNOSTIC_COMPLETE', parts: uploaded, stopAt: new Date(stopAt).toISOString(), counted: false };
}

interface ContinuousAssetIdentity {
  assetId: number; assetName: string; assetBytes: number; assetDigest: string; archiveSha256: string;
}
interface ContinuousBlockDraft {
  schemaVersion: 1; kind: 'CONTINUOUS_BLOCK'; acquisitionPolicyHash: string;
  attemptId: string; diagnosticOnly: boolean; plan: StudyBlockPlan | null;
  phase: 'DEVELOPMENT' | 'HOLDOUT' | null;
  replayConfigHash: string; simulatorHashes: Record<string, string>;
  checkpoints: Array<ContinuousAssetIdentity & { index: number; checkpointHash: string; lastReceivedAt: string }>;
}
interface ContinuousReference {
  schemaVersion: 1; kind: 'CONTINUOUS_WINDOW'; acquisitionPolicyHash: string;
  source: ContinuousAssetIdentity; runId: string; manifestHash: string; recordingHash: string;
}
// Acquisition v2 superseded v1's separate quality-gated smoke and per-window reconnects.
// Keep its hash readable so immutable v2 window references remain replayable after capacity changes.
export const CONTINUOUS_CAPTURE_POLICY_V2_HASH = hashStudyValue({
  version: 2, intervalMs: 300_000, maxPendingCheckpoints: 2, uploadDeadlineMs: 120_000,
  rawRunScope: 'owned-block', startup: 'in-band-subscription-health',
  qualityFailure: 'archive-and-continue', scientificWindows: 'original-30-minute-bounds',
} as const);
// Two Sep 22 blocks reached 1 GiB; the faster one did so after about 4.40 hours. 1.5 GiB
// covers five hours at that observed rate with more than 25% headroom and retains a hard cap.
export const CONTINUOUS_CAPTURE_MAX_BYTES = 1536 * 1024 * 1024;
// The scientific ledger and its frozen 99%/80% criteria remain compatible and immutable.
export const continuousCapturePolicy = {
  version: 3, intervalMs: 300_000, maxPendingCheckpoints: 2, uploadDeadlineMs: 120_000,
  maxBytes: CONTINUOUS_CAPTURE_MAX_BYTES,
  rawRunScope: 'owned-block', startup: 'in-band-subscription-health',
  qualityFailure: 'archive-and-continue', scientificWindows: 'original-30-minute-bounds',
} as const;
export const CONTINUOUS_CAPTURE_POLICY_HASH = hashStudyValue(continuousCapturePolicy);
function supportedContinuousPolicyHash(value: string): boolean {
  return value === CONTINUOUS_CAPTURE_POLICY_HASH || value === CONTINUOUS_CAPTURE_POLICY_V2_HASH;
}
function continuousAsset(asset: ReleaseAsset, archive: string): ContinuousAssetIdentity {
  const archiveSha256 = sha256File(archive);
  return { assetId: asset.id, assetName: asset.name, assetBytes: asset.size,
    assetDigest: asset.digest ?? `sha256:${archiveSha256}`, archiveSha256 };
}

/** Operational status is best-effort before archival; it must never prevent raw preservation. */
export async function preserveContinuousRaw<T>(persistStopped: () => Promise<void>, preserveRaw: () => Promise<T>): Promise<T> {
  try { await persistStopped(); } catch { /* The mandatory raw archive still runs. */ }
  return preserveRaw();
}
function validateContinuousAsset(value: ContinuousAssetIdentity): void {
  if (!Number.isSafeInteger(value.assetId) || value.assetId <= 0 || !Number.isSafeInteger(value.assetBytes) || value.assetBytes <= 0
    || !/^[a-zA-Z0-9_.-]+\.tar\.gz$/.test(value.assetName) || !/^[a-f0-9]{64}$/.test(value.archiveSha256)
    || value.assetDigest !== `sha256:${value.archiveSha256}`) throw new Error('Invalid continuous asset identity');
}
async function extractContinuousAsset(repository: string, source: ContinuousAssetIdentity, workspace: string): Promise<string> {
  validateContinuousAsset(source);
  const destination = path.join(workspace, `continuous-asset-${source.assetId}`);
  const marker = path.join(destination, '.verified.json');
  if (existsSync(marker)) {
    const cached = JSON.parse(readFileSync(marker, 'utf8')) as { sha256: string; root: string };
    if (cached.sha256 !== source.archiveSha256 || path.basename(cached.root) !== cached.root) throw new Error('Continuous cache identity conflict');
    return realpathSync(path.join(destination, cached.root));
  }
  mkdirSync(destination);
  const archive = path.join(destination, source.assetName);
  await downloadAsset(repository, source.assetId, archive);
  if (statSync(archive).size !== source.assetBytes || sha256File(archive) !== source.archiveSha256) throw new Error('Continuous source archive changed');
  const listed = await runFile('tar', ['-tzf', archive], { maxBuffer: 16 * 1024 * 1024 });
  const entries = safeArchiveEntries(listed.stdout.toString('utf8'));
  const roots = [...new Set(entries.map(entry => entry.split('/')[0]).filter(Boolean))];
  if (roots.length !== 1) throw new Error('Continuous archive requires one root');
  await runFile('tar', ['-xzf', archive, '-C', destination]);
  const directory = realpathSync(path.join(destination, roots[0]));
  if (!directory.startsWith(`${realpathSync(destination)}${path.sep}`)) throw new Error('Continuous extraction escaped workspace');
  atomicJson(marker, { sha256: source.archiveSha256, root: roots[0] });
  return directory;
}

/** Capture never waits for a previous segment's archive. The recorder owns bounded backpressure. */
async function recordContinuousStudy(repository: string, workspace: string, args: RecorderArguments,
  draft: Omit<ContinuousBlockDraft, 'checkpoints'>, operation?: BlockOperation) {
  let previousCheckpointHash: string | null = null;
  const checkpoints: ContinuousBlockDraft['checkpoints'] = [];
  const checkpointDirectories: string[] = [];
  const directory = await recordMarket({ ...args, maxBytes: continuousCapturePolicy.maxBytes,
    checkpointIntervalMs: args.checkpointIntervalMs ?? continuousCapturePolicy.intervalMs,
    checkpointMaxPending: continuousCapturePolicy.maxPendingCheckpoints,
    checkpointUploadTimeoutMs: continuousCapturePolicy.uploadDeadlineMs,
    checkpointDrainTimeoutMs: continuousCapturePolicy.uploadDeadlineMs,
    onCheckpoint: async (checkpoint, signal) => {
      const checkpointDirectory = path.join(path.resolve(workspace), `checkpoint-${checkpoint.manifest.runId}-${checkpoint.segment.index}`);
      const prepared = prepareContinuousCheckpoint(checkpoint.directory, checkpoint.segment,
        { ...draft, protocolHash: STUDY_PROTOCOL_HASH }, previousCheckpointHash, checkpointDirectory, checkpoint.manifest);
      const name = `checkpoint-${checkpoint.manifest.runId}-${String(checkpoint.segment.index).padStart(6, '0')}.tar.gz`;
      const archive = await archiveChunk(prepared.directory, name, undefined, signal);
      const asset = await uploadConfirmedAsset(repository, archive, signal);
      checkpoints.push({ ...continuousAsset(asset, archive), index: checkpoint.segment.index,
        checkpointHash: prepared.checkpointHash, lastReceivedAt: checkpoint.segment.lastReceivedAt });
      previousCheckpointHash = prepared.checkpointHash;
      checkpointDirectories.push(prepared.directory);
      if (operation) {
        confirmOperationCheckpoint(operation, { index: checkpoint.segment.index, assetId: asset.id,
          lastReceivedAt: checkpoint.segment.lastReceivedAt, confirmedAt: new Date().toISOString() });
        const current = operation.plan.chunks.find(chunk => Date.parse(chunk.plannedEnd) > Date.parse(checkpoint.segment.lastReceivedAt));
        operation.currentChunkIndex = current?.index;
        await saveOperation(repository, operation);
      }
      appendStudySummary(`Контрольная точка ${checkpoint.segment.index}: приватный архив подтверждён, поток продолжается.`);
    },
  }, process.cwd());
  const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as ObservationManifest;
  let persistStopped = async () => {};
  if (operation && continuousRecordingCompleted(manifest.status, manifest.capture?.reason)) {
    markOperationRecordingStopped(operation, manifest.completedAt ?? new Date().toISOString());
    persistStopped = () => saveOperation(repository, operation);
  }
  const block: ContinuousBlockDraft = { ...draft, checkpoints };
  atomicJson(path.join(directory, 'continuous-block.json'), block);
  return preserveContinuousRaw(persistStopped, async () => {
    // Preserve raw input even if acquisition or later scientific processing failed.
    const archive = await archiveChunk(directory, `continuous-block-${manifest.runId}.tar.gz`);
    const asset = await uploadConfirmedAsset(repository, archive);
    return { directory, block, manifest, source: continuousAsset(asset, archive), checkpointDirectories };
  });
}

/** Scientific windows refer to one immutable run; they do not duplicate events or reset replay. */
async function publishContinuousWindows(repository: string, directory: string, source: ContinuousAssetIdentity,
  draft: ContinuousBlockDraft, workspace: string): Promise<number> {
  if (draft.diagnosticOnly || !draft.plan || !draft.phase) return 0;
  if (!supportedContinuousPolicyHash(draft.acquisitionPolicyHash) || draft.replayConfigHash !== replayConfigHash()
    || !sameHashes(draft.simulatorHashes, replaySourceHashes(process.cwd()))) throw new Error('Continuous processing requires its recorded implementation');
  const plan = draft.plan, identity = readManifestIdentity(directory);
  const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as ObservationManifest;
  if (manifest.instruments.length !== STUDY_TICKERS.length || STUDY_TICKERS.some(ticker => !manifest.instruments.some(item => item.ticker === ticker))) {
    throw new Error('Continuous study instrument set differs from protocol');
  }
  const assessments = await assessContinuousWindows(directory, plan.chunks);
  let ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value, saved = 0;
  for (const assessment of assessments) {
    const assetName = `study-${plan.sessionDate}-${plan.block}-${String(assessment.index).padStart(2, '0')}-${draft.attemptId.replace(':', '-')}.tar.gz`;
    if (ledger.chunks.some(item => item.assetName === assetName)) continue;
    const view = path.join(workspace, `window-${identity.runId}-${assessment.index}`);
    mkdirSync(view, { recursive: true });
    const reference: ContinuousReference = { schemaVersion: 1, kind: 'CONTINUOUS_WINDOW',
      acquisitionPolicyHash: draft.acquisitionPolicyHash, source, ...identity };
    const receiptDraft: ChunkDraft = { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, assetName,
      releaseTag: STUDY_RELEASE_TAG, attemptId: draft.attemptId,
      chunkId: `${plan.sessionDate}:${plan.block}:${assessment.index}`, sessionDate: plan.sessionDate,
      phase: draft.phase, block: plan.block, chunkIndex: assessment.index,
      plannedStart: assessment.plannedStart, plannedEnd: assessment.plannedEnd, ...identity,
      replayConfigHash: draft.replayConfigHash, simulatorHashes: draft.simulatorHashes,
      quality: assessment.quality, uploadedAt: manifest.completedAt!, acquisitionPolicyHash: draft.acquisitionPolicyHash,
      ...(plan.recovery ? { collection: studyCollectionMetadata(plan) } : {}) };
    atomicJson(path.join(view, 'continuous-reference.json'), reference);
    atomicJson(path.join(view, 'window-quality.json'), assessment);
    atomicJson(path.join(view, 'study-chunk-receipt.json'), receiptDraft);
    const archive = await archiveChunk(view, assetName);
    const asset = await uploadConfirmedAsset(repository, archive);
    const receipt: StudyChunkReceipt = { ...receiptDraft, ...continuousAsset(asset, archive) };
    ledger = await updateRemoteLedger(repository, current => acceptStudyChunk(current, receipt), `Accept continuous window ${receipt.chunkId}`);
    saved += 1;
  }
  return saved;
}

async function syncRecoveredOperation(repository: string, attemptId: string, ledger: StudyLedger): Promise<void> {
  const file = `operations/${attemptId.replace(':', '-')}.json`, remote = await readRemoteText(repository, file);
  if (!remote.value) return;
  const operation = JSON.parse(remote.value) as BlockOperation;
  if (operation.attemptId !== attemptId || operation.state === 'STARTING' || operation.state === 'CAPTURING') return;
  let changed = false;
  for (const receipt of ledger.chunks.filter(item => item.attemptId === attemptId)) {
    if (!operation.parts.some(part => part.index === receipt.chunkIndex || part.assetId === receipt.assetId)) {
      operation.parts.push({ index: receipt.chunkIndex, status: 'SAVED', assetId: receipt.assetId,
        quality: receipt.quality, reasons: [] });
      changed = true;
    }
  }
  const complete = operation.plan.chunks.every(chunk => ledger.chunks.some(receipt => receipt.attemptId === attemptId
    && receipt.block === operation.plan.block && receipt.chunkIndex === chunk.index
    && receipt.plannedStart === chunk.plannedStart && receipt.plannedEnd === chunk.plannedEnd));
  if (complete && operation.state === 'PROCESSING') { operation.state = 'FINISHED'; changed = true; }
  if (changed) await saveOperation(repository, operation);
}

/** Recovery of a finished raw upload before all scientific references were committed. */
export async function reconcileContinuousBlocks(repository: string, workspace: string): Promise<number> {
  let failures = 0;
  let ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
  for (const asset of (await releaseAssets(repository)).filter(item => item.name.startsWith('continuous-block-'))) {
    try {
    // Every source has a private completion marker only after all window receipts are committed.
    const markerPath = `continuous-completed/${asset.id}.json`;
    const marker = await readRemoteText(repository, markerPath);
    if (marker.value) {
      const saved = JSON.parse(marker.value) as { attemptId?: string };
      if (saved.attemptId) await syncRecoveredOperation(repository, saved.attemptId, ledger);
      continue;
    }
    const archive = path.join(workspace, asset.name);
    await downloadAsset(repository, asset.id, archive);
    const source = continuousAsset(asset, archive);
    if (source.assetDigest !== `sha256:${source.archiveSha256}` || statSync(archive).size !== asset.size) throw new Error('Continuous recovery digest mismatch');
    const directory = await extractContinuousAsset(repository, source, workspace);
    const draft = JSON.parse(readFileSync(path.join(directory, 'continuous-block.json'), 'utf8')) as ContinuousBlockDraft;
    const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as ObservationManifest;
    if (draft.schemaVersion !== 1 || draft.kind !== 'CONTINUOUS_BLOCK') throw new Error('Invalid continuous block provenance');
    if (!draft.diagnosticOnly && manifest.status === 'COMPLETE') {
      if (!ledger.attempts.some(item => item.attemptId === draft.attemptId)) throw new Error('Continuous block has no persisted attempt');
      await publishContinuousWindows(repository, directory, source, draft, workspace);
      ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
      const plan = draft.plan;
      if (!plan || !plan.chunks.every(chunk => ledger.chunks.some(receipt => receipt.attemptId === draft.attemptId
        && receipt.block === plan.block && receipt.chunkIndex === chunk.index
        && receipt.plannedStart === chunk.plannedStart && receipt.plannedEnd === chunk.plannedEnd))) {
        throw new Error('Continuous windows are not fully committed');
      }
    }
    await putRemoteFile(repository, markerPath, `${JSON.stringify({ assetId: asset.id, archiveSha256: source.archiveSha256,
      attemptId: draft.attemptId, diagnosticOnly: draft.diagnosticOnly, status: manifest.status })}\n`, null, 'Confirm continuous block recovery');
    if (!draft.diagnosticOnly && manifest.status === 'COMPLETE') await syncRecoveredOperation(repository, draft.attemptId, ledger);
    } catch {
      failures += 1;
      appendStudySummary(`Архив ${asset.id}: обработка не восстановлена; источник сохранён и не засчитан. Остальные сводки продолжаются.`);
    }
  }
  return failures;
}

async function continuousBlockProcessing(repository: string, ledger: StudyLedger, plan: StudyBlockPlan): Promise<boolean> {
  for (const attempt of ledger.attempts.filter(item => item.sessionDate === plan.sessionDate && item.block === plan.block)) {
    const remote = await readRemoteText(repository, `operations/${attempt.attemptId.replace(':', '-')}.json`);
    if (!remote.value) continue;
    const operation = JSON.parse(remote.value) as BlockOperation;
    if (operation.attemptId === attempt.attemptId && operation.state === 'PROCESSING') return true;
  }
  return false;
}

export async function captureContinuousStudyBlock(repository: string, block: StudyBlock, workspace: string,
  runId: string, runAttempt: number, signal: AbortSignal): Promise<BlockResult> {
  if (process.env.MARKET_STUDY_ENABLED !== 'true') throw new Error('Market study capture is paused');
  const discovered = await discoverBlockPlan(block, signal);
  if (!discovered) return { action: 'SKIPPED', reason: 'OUTSIDE_BLOCK_WINDOW', chunks: 0, reportPath: null };
  mkdirSync(workspace, { recursive: true });
  await ensureDraftRelease(repository);
  const recovered = await reconcileReleaseAssets(repository);
  const plan = planRecoverableStudyBlock(discovered.sessionDate, discovered.mainStart, discovered.mainEnd, block, Date.now());
  if (!plan) return { action: 'SKIPPED', reason: 'OUTSIDE_BLOCK_WINDOW', chunks: 0, reportPath: null };
  const decision = planStudyRun(recovered, { event: 'schedule', campaignEnabled: true, runId, runAttempt, sessionDate: plan.sessionDate });
  if (decision.action !== 'CAPTURE') return { action: 'SKIPPED', reason: decision.action, chunks: 0, reportPath: null };
  if (await continuousBlockProcessing(repository, recovered, plan)) {
    return { action: 'SKIPPED', reason: 'BLOCK_PROCESSING', chunks: 0, reportPath: null };
  }
  if (studyBlockRecorded(recovered, plan)) return { action: 'SKIPPED', reason: 'BLOCK_ALREADY_RECORDED', chunks: 0,
    reportPath: await maybeFinalizeStudyDay(repository, plan, workspace) };
  assertFrozenRuntime(recovered);
  const remaining = remainingStudyChunks(plan, recovered, Date.now());
  if (!remaining.length) return { action: 'SKIPPED', reason: 'NO_REMAINING_WINDOWS', chunks: 0, reportPath: null };
  const attemptId = `${runId}:${runAttempt}`;
  const ledger = await updateRemoteLedger(repository, current => beginStudyAttempt(current, { runId, runAttempt,
    sessionDate: plan.sessionDate, block: plan.block, mode: 'COUNTED', startedAt: new Date().toISOString() }), 'Begin continuous capture');
  const phase = ledger.attempts.find(item => item.attemptId === attemptId)?.phase;
  if (!phase) throw new Error('Continuous attempt phase missing');
  const operation: BlockOperation = { schemaVersion: 1, attemptId, plan, startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(), state: 'STARTING', failure: null, parts: [], captureFormat: 'continuous-v2',
    checkpointIntervalMs: continuousCapturePolicy.intervalMs, checkpoints: [] };
  await saveOperation(repository, operation);
  try {
    // This authenticated private write precedes acquisition; market quality is evaluated in-band.
    const probe = mkdtempSync(path.join(workspace, 'continuous-start-'));
    atomicJson(path.join(probe, 'startup.json'), { attemptId, acquisitionPolicyHash: CONTINUOUS_CAPTURE_POLICY_HASH, diagnosticOnly: true });
    await uploadConfirmedAsset(repository, await archiveChunk(probe, `continuous-start-${runId}-${runAttempt}.tar.gz`), signal);
    await waitUntil(Math.max(Date.parse(plan.captureNotBefore), Date.parse(remaining[0].plannedStart)), signal);
    const stopAt = Date.parse(plan.ownedEnd) - STUDY_CHUNK_END_MARGIN_MS;
    if (stopAt <= Date.now()) throw new Error('Continuous block expired during preparation');
    operation.state = 'CAPTURING'; operation.captureStartedAt = new Date().toISOString(); operation.currentChunkIndex = remaining[0].index; await saveOperation(repository, operation);
    const result = await recordContinuousStudy(repository, workspace, { ...recorderArguments(workspace,
      Math.ceil((stopAt - Date.now()) / 1000), 'main'), captureDeadlineMs: stopAt, parentSignal: signal, setExitCodeOnFailure: false },
    { schemaVersion: 1, kind: 'CONTINUOUS_BLOCK', acquisitionPolicyHash: CONTINUOUS_CAPTURE_POLICY_HASH,
      attemptId, diagnosticOnly: false, plan, phase, replayConfigHash: replayConfigHash(), simulatorHashes: replaySourceHashes(process.cwd()) }, operation);
    if (result.manifest.status !== 'COMPLETE' || result.manifest.capture?.reason !== 'duration') throw new Error('Continuous capture incomplete; durable raw evidence preserved');
    operation.state = 'PROCESSING'; await saveOperation(repository, operation);
    return { action: 'CAPTURED', plan, chunks: 0, reportPath: null,
      rawBlockConfirmed: true, scientificProcessing: 'DEFERRED_TO_REPORT' };
  } catch (error) {
    operation.state = signal.aborted ? 'CANCELLED' : 'FAILED';
    operation.failure = continuousOperationFailureStage(operation);
    try { await saveOperation(repository, operation); } catch { /* Private release assets remain authoritative. */ }
    appendStudySummary('Непрерывный сбор прерван. Подтверждённые контрольные точки сохранены; неполный день не засчитывается.');
    throw error;
  }
}

async function continuousPilot(repository: string, workspace: string, signal: AbortSignal) {
  mkdirSync(workspace, { recursive: true });
  await ensureDraftRelease(repository);
  const result = await recordContinuousStudy(repository, workspace, { ...recorderArguments(workspace, 180, 'any'),
    checkpointIntervalMs: 30_000, captureDeadlineMs: Date.now() + 180_000, parentSignal: signal, setExitCodeOnFailure: false }, {
    schemaVersion: 1, kind: 'CONTINUOUS_BLOCK', acquisitionPolicyHash: CONTINUOUS_CAPTURE_POLICY_HASH,
    attemptId: `${process.env.GITHUB_RUN_ID ?? '0'}:${process.env.GITHUB_RUN_ATTEMPT ?? '1'}`,
    diagnosticOnly: true, plan: null, phase: null, replayConfigHash: replayConfigHash(), simulatorHashes: replaySourceHashes(process.cwd()),
  });
  if (result.manifest.status !== 'COMPLETE' || result.manifest.capture?.reason !== 'duration' || result.block.checkpoints.length < 2) {
    throw new Error('Continuous pilot capture failed; confirmed checkpoints retained');
  }
  const verification = mkdtempSync(path.join(workspace, 'remote-verification-'));
  const remoteRaw = await extractContinuousAsset(repository, result.source, verification);
  const remoteManifest = JSON.parse(readFileSync(path.join(remoteRaw, 'manifest.json'), 'utf8')) as ObservationManifest;
  const remoteSegments: string[] = [];
  for (const source of result.block.checkpoints) remoteSegments.push(await extractContinuousAsset(repository, source, verification));
  const restored = await restoreContinuousCheckpoints(remoteSegments, path.join(verification, 'restored'), path.join(remoteRaw, 'manifest.json'));
  if (!restored.complete || restored.recording.sha256 !== remoteManifest.recording?.sha256
    || readManifestIdentity(restored.directory).manifestHash !== readManifestIdentity(remoteRaw).manifestHash) {
    throw new Error('Remote checkpoint reconstruction differs from raw run');
  }
  const summary = JSON.parse(readFileSync(path.join(remoteRaw, 'summary.json'), 'utf8')) as JsonObject;
  if (summary.allSubscriptionsAcknowledged !== true) throw new Error('Pilot subscription health failed');
  const replay = await replayRecording(restored.directory, path.join(verification, 'replay'));
  const evidence = { schemaVersion: 1, action: 'CONTINUOUS_PILOT_VERIFIED', counted: false,
    acquisitionPolicyHash: CONTINUOUS_CAPTURE_POLICY_HASH, source: result.source,
    checkpoints: result.block.checkpoints, reconstructionHash: restored.recording.sha256,
    manifestHash: readManifestIdentity(restored.directory).manifestHash,
    events: restored.recording.events, connections: remoteManifest.capture?.epochs,
    capture: remoteManifest.capture, allSubscriptionsAcknowledged: true,
    replayQuality: replay.quality, verifiedAt: new Date().toISOString() };
  atomicJson(path.join(verification, 'pilot-verification.json'), evidence);
  const evidenceDirectory = mkdtempSync(path.join(workspace, 'pilot-evidence-'));
  atomicJson(path.join(evidenceDirectory, 'pilot-verification.json'), evidence);
  await uploadConfirmedAsset(repository, await archiveChunk(evidenceDirectory, `continuous-pilot-proof-${remoteManifest.runId}.tar.gz`));
  appendStudySummary(`Непрерывный прогон проверен: ${result.block.checkpoints.length} архивов скачаны из приватного GitHub и восстановлены без изменения событий. Повторный анализ завершён. Качество данных: **${replay.quality.status}**. Это технический тест, не полный торговый день.`);
  return evidence;
}

async function main(argv = process.argv.slice(2)): Promise<void> {
  const { command, values } = parseArguments(argv);
  const controller = new AbortController(), stop = () => controller.abort();
  process.on('SIGINT', stop); process.on('SIGTERM', stop);
  try {
    if (command === 'prepare-block') {
      const block = required(values, '--block');
      const preparation = planStudyPreparation(block as StudyBlock, Date.now(),
        process.env.GITHUB_EVENT_NAME === 'schedule' ? 'schedule' : 'manual');
      console.log(JSON.stringify({ action: 'WAITING_FOR_BLOCK', block, ...preparation }));
      await waitUntil(Date.parse(preparation.readyAt), controller.signal);
      controller.signal.throwIfAborted();
      assertStudyPreparationReady(preparation, Date.now());
      console.log(JSON.stringify({ action: 'PREPARATION_COMPLETE', block, ...preparation }));
      return;
    }
    if (command === 'preflight') {
      const result = await preflight(path.resolve(required(values, '--workspace')), required(values, '--repo'), controller.signal);
      console.log(JSON.stringify(result)); return;
    }
    if (command === 'smoke') {
      const directory = await checkedSmoke(path.resolve(required(values, '--workspace')), values.get('--repo'), controller.signal);
      output('action', 'SMOKE_COMPLETE'); output('report_path', directory); console.log(directory); return;
    }
    if (command === 'continuous-pilot') {
      const result = await continuousPilot(required(values, '--repo'), path.resolve(required(values, '--workspace')), controller.signal);
      output('action', result.action); console.log(JSON.stringify(result)); return;
    }
    if (command === 'observe') {
      const result = await observeMorning(path.resolve(required(values, '--workspace')), required(values, '--repo'), controller.signal);
      output('action', result.action); console.log(JSON.stringify(result)); return;
    }
    const repository = required(values, '--repo');
    if (command === 'report') {
      const dates = await refreshOperationalReports(repository);
      console.log(JSON.stringify({ action: 'OPERATIONAL_REPORTS_UPDATED', dates })); return;
    }
    if (command === 'research') {
      const dates = await refreshDailyResearchReports(repository);
      console.log(JSON.stringify({ action: 'DAILY_RESEARCH_UPDATED', dates })); return;
    }
    if (command === 'status') {
      const ledger = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
      console.log(JSON.stringify({ phase: ledger.phase, attempts: ledger.attempts.length, chunks: ledger.chunks.length, days: ledger.days.length })); return;
    }
    if (command === 'freeze') {
      const ledger = await updateRemoteLedger(repository, current => freezeStudy(current, { frozenAt: new Date().toISOString(),
        replayConfigHash: replayConfigHash(), simulatorHashes: replaySourceHashes(process.cwd()) }), 'Freeze market study protocol');
      console.log(JSON.stringify({ phase: ledger.phase })); return;
    }
    if (command === 'run-block') {
      const block = required(values, '--block'); if (!['early', 'late'].includes(block)) throw new Error('Invalid study block');
      const result = await captureContinuousStudyBlock(repository, block as StudyBlock, path.resolve(required(values, '--workspace')),
        required(values, '--run-id'), Number(required(values, '--run-attempt')), controller.signal);
      output('action', result.action); if (result.reportPath) output('report_path', result.reportPath);
      if (process.env.GITHUB_STEP_SUMMARY) {
        const summary = result.action === 'SKIPPED'
          ? `### Сбор пропущен\n\nПричина: \`${result.reason}\`. Эта задача не создала новую попытку записи. Успех служебного запуска не означает собранный торговый день.\n`
          : result.scientificProcessing === 'DEFERRED_TO_REPORT'
            ? '### Исходный блок подтверждён\n\nНепрерывный архив сохранён приватно. Научные окна обрабатываются отдельной восстанавливаемой задачей; до её завершения блок не считается полностью обработанным.\n'
            : `### Блок записан\n\nПодтверждено новых частей: ${result.chunks}. Полнота дня оценивается отдельной проверкой данных.\n`;
        writeFileSync(process.env.GITHUB_STEP_SUMMARY, summary, { flag: 'a' });
      }
      console.log(JSON.stringify(result)); return;
    }
    throw new Error('Expected prepare-block, preflight, smoke, observe, continuous-pilot, run-block, freeze, report, research, or status command');
  } finally {
    controller.abort(); process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(() => { console.error('Market study command failed; inspect the workflow summary and immutable archive.'); process.exitCode = 1; });
}
