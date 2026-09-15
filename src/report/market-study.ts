import { createHash, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TinkoffInvestApi } from 'tinkoff-invest-api';
import { TINKOFF_SANDBOX_ENDPOINT } from '../core/tinkoff-client.js';
import { recordMarket, type RecorderArguments } from './record-market.js';
import { replayConfigHash, replayRecording, replaySession, replaySourceHashes } from './replay-orderbook.js';
import { discoverMarketPilot } from '../research/market-pilot-runner.js';
import { nextMainSessionWindow } from '../research/observation-session.js';
import {
  STUDY_PROTOCOL_HASH, STUDY_RELEASE_TAG, STUDY_STATE_BRANCH, STUDY_TICKERS, assertStudyPreparationReady, canonicalJson, chunkDurationSeconds, hashStudyValue, planStudyBlock, planStudyPreparation,
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

function runFile(command: string, args: string[], options: { cwd?: string; maxBuffer?: number } = {}): Promise<ExecResult> {
  return new Promise((resolve, reject) => execFile(command, args, {
    cwd: options.cwd, encoding: 'buffer', maxBuffer: options.maxBuffer ?? 512 * 1024 * 1024,
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
async function ghJson(args: string[]): Promise<JsonObject> {
  const { stdout } = await runFile('gh', args); return JSON.parse(stdout.toString('utf8')) as JsonObject;
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
  return planStudyBlock(moscowDate, window.start, window.end, block, now);
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
async function releaseAssets(repository: string): Promise<ReleaseAsset[]> {
  const release = await findDraftRelease(repository);
  if (!release) throw new Error('Archive draft is missing from the release inventory');
  const result: ReleaseAsset[] = [];
  for (let page = 1; page <= 10; page += 1) {
    const response = await ghJson(['api', `/repos/${repository}/releases/${Number(release.id)}/assets?per_page=100&page=${page}`]);
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
  throw new Error('Draft release reached its supported 1000-asset inventory');
}
async function downloadAsset(repository: string, assetId: number, destination: string): Promise<void> {
  const { stdout } = await runFile('gh', ['api', '-H', 'Accept: application/octet-stream',
    `/repos/${repository}/releases/assets/${assetId}`]);
  writeFileSync(destination, stdout, { mode: 0o600 });
}
async function uploadConfirmedAsset(repository: string, archive: string): Promise<ReleaseAsset> {
  await ensureDraftRelease(repository);
  await runFile('gh', ['release', 'upload', STUDY_RELEASE_TAG, archive, '--repo', repository]);
  const asset = (await releaseAssets(repository)).find(item => item.name === path.basename(archive));
  if (!asset) throw new Error('Uploaded release asset was not confirmed');
  const expected = `sha256:${sha256File(archive)}`;
  if (asset.digest && asset.digest !== expected) throw new Error('Remote release asset digest differs from local archive');
  if (!asset.digest) {
    const downloaded = path.join(tmpdir(), `market-study-verify-${asset.id}.tar.gz`);
    await downloadAsset(repository, asset.id, downloaded);
    if (sha256File(downloaded) !== expected.slice(7)) throw new Error('Downloaded release asset differs from local archive');
    unlinkSync(downloaded);
  }
  return asset;
}

interface ChunkDraft extends Omit<StudyChunkReceipt, 'assetId' | 'assetDigest' | 'assetBytes' | 'archiveSha256'> {}
interface DayDraft extends Omit<StudyDayReceipt,
  'reportAssetId' | 'reportAssetName' | 'reportAssetDigest' | 'reportAssetBytes' | 'reportArchiveSha256'> {}
function readManifestIdentity(directory: string): { runId: string; manifestHash: string; recordingHash: string } {
  const file = path.join(directory, 'manifest.json'), bytes = readFileSync(file);
  const manifest = JSON.parse(bytes.toString('utf8')) as JsonObject;
  const recording = manifest.recording as JsonObject | undefined;
  if (typeof manifest.runId !== 'string' || typeof recording?.sha256 !== 'string') throw new Error('Completed chunk identity is missing');
  return { runId: manifest.runId, manifestHash: createHash('sha256').update(bytes).digest('hex'), recordingHash: recording.sha256 };
}
async function archiveChunk(directory: string, name: string): Promise<string> {
  const archive = path.join(path.dirname(directory), name);
  await runFile('tar', ['-czf', archive, '-C', path.dirname(directory), path.basename(directory)]);
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
  const candidates = (await releaseAssets(repository)).filter(asset => asset.name.startsWith('study-') && !known.has(asset.id));
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

async function materializeAsset(repository: string, receipt: StudyChunkReceipt, workspace: string): Promise<string> {
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
  const identity = readManifestIdentity(directory);
  if (identity.manifestHash !== receipt.manifestHash || identity.recordingHash !== receipt.recordingHash) {
    throw new Error('Extracted chunk identity differs from its immutable receipt');
  }
  return directory;
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
    const directories: string[] = [];
    for (const receipt of receipts) directories.push(await materializeAsset(repository, receipt, materialized));
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

type BlockResult = { action: 'CAPTURED'; plan: StudyBlockPlan; chunks: number; reportPath: string | null }
  | { action: 'SKIPPED'; reason: string; chunks: 0; reportPath: string | null };
export async function captureStudyBlock(repository: string, block: StudyBlock, workspace: string,
  runId: string, runAttempt: number, signal: AbortSignal): Promise<BlockResult> {
  if (process.env.MARKET_STUDY_ENABLED !== 'true') throw new Error('Full-session campaign is paused pending storage configuration, sandbox smoke and activation');
  const plan = await discoverBlockPlan(block, signal);
  if (!plan) return { action: 'SKIPPED', reason: 'OUTSIDE_BLOCK_WINDOW', chunks: 0, reportPath: null };
  mkdirSync(path.resolve(workspace), { recursive: true });
  await ensureDraftRelease(repository);
  const recovered = await reconcileReleaseAssets(repository);
  const decision = planStudyRun(recovered, { event: 'schedule', campaignEnabled: true, runId, runAttempt,
    sessionDate: plan.sessionDate });
  if (decision.action !== 'CAPTURE') return { action: 'SKIPPED', reason: decision.action, chunks: 0, reportPath: null };
  if (studyBlockRecorded(recovered, plan)) return { action: 'SKIPPED', reason: 'BLOCK_ALREADY_RECORDED', chunks: 0,
    reportPath: await maybeFinalizeStudyDay(repository, plan, workspace) };
  assertFrozenRuntime(recovered);
  const attemptId = `${runId}:${runAttempt}`;
  let persisted = await updateRemoteLedger(repository, ledger => beginStudyAttempt(ledger, { runId, runAttempt,
    sessionDate: plan.sessionDate, block, mode: 'COUNTED', startedAt: new Date().toISOString() }), `Begin ${plan.sessionDate} ${block}`);
  assertFrozenRuntime(persisted);
  return runAfterBlockCheck(Date.parse(plan.captureNotBefore), signal, async () => {
    console.log('Automatic block check: recording 60 seconds of exchange main-session data.');
    await smoke(workspace, repository);
    console.log('Automatic block check passed: quality, replay and private archive confirmed.');
  }, async () => {
    let uploaded = 0;
    for (const chunk of plan.chunks) {
      if (signal.aborted) throw new Error('Study block aborted');
      await waitUntil(Date.parse(chunk.plannedStart), signal);
      const seconds = chunkDurationSeconds(chunk, Date.now());
      if (seconds <= 0) continue;
      persisted = (await readRemoteFile<StudyLedger>(repository, STATE_PATH)).value;
      assertFrozenRuntime(persisted);
      if (studyChunkRecorded(persisted, plan, chunk)) continue;
      const directory = await recordMarket(recorderArguments(workspace, seconds, 'main'), process.cwd());
      const quality = assessStudyChunk(directory, chunk), identity = readManifestIdentity(directory);
      const assetName = `study-${plan.sessionDate}-${block}-${String(chunk.index).padStart(2, '0')}-${runId}-${runAttempt}.tar.gz`;
      const draft: ChunkDraft = { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, assetName,
        releaseTag: STUDY_RELEASE_TAG, attemptId, chunkId: `${plan.sessionDate}:${block}:${chunk.index}`,
        sessionDate: plan.sessionDate, phase: 'DEVELOPMENT', block, chunkIndex: chunk.index,
        plannedStart: chunk.plannedStart, plannedEnd: chunk.plannedEnd, runId: identity.runId,
        manifestHash: identity.manifestHash, recordingHash: identity.recordingHash,
        replayConfigHash: replayConfigHash(), simulatorHashes: replaySourceHashes(process.cwd()), quality: quality.status,
        uploadedAt: new Date().toISOString() };
      const attempt = persisted.attempts.find(item => item.attemptId === attemptId);
      if (!attempt?.phase) throw new Error('Persisted study attempt disappeared');
      draft.phase = attempt.phase;
      atomicJson(path.join(directory, 'study-chunk-receipt.json'), draft);
      const archive = await archiveChunk(directory, assetName), archiveHash = sha256File(archive);
      const asset = await uploadConfirmedAsset(repository, archive);
      const receipt: StudyChunkReceipt = { ...draft, assetId: asset.id, assetDigest: asset.digest ?? `sha256:${archiveHash}`,
        assetBytes: asset.size || statSync(archive).size, archiveSha256: archiveHash };
      await updateRemoteLedger(repository, current => acceptStudyChunk(current, receipt), `Accept ${receipt.chunkId}`);
      uploaded += 1;
    }
    const reportPath = await maybeFinalizeStudyDay(repository, plan, workspace);
    return { action: 'CAPTURED' as const, plan, chunks: uploaded, reportPath };
  });
}

async function smoke(workspace: string, repository?: string): Promise<string> {
  const directory = await recordMarket(recorderArguments(workspace, 60, 'main'), process.cwd());
  const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as JsonObject;
  if (manifest.status !== 'COMPLETE') throw new Error('Smoke capture did not complete');
  const quality = assessStudyChunk(directory);
  if (quality.status !== 'PASS') throw new Error('Smoke capture failed the collector quality gate');
  const replayOutput = `${directory}-replay`;
  const replay = await replayRecording(directory, replayOutput);
  atomicJson(path.join(directory, 'study-smoke.json'), { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH,
    diagnosticOnly: true, counted: false, status: manifest.status, quality, replayQuality: replay.quality,
    replayConfigHash: replay.configHash, completedAt: new Date().toISOString() });
  if (repository) {
    const identity = readManifestIdentity(directory);
    const archive = await archiveChunk(directory, `smoke-${identity.runId}.tar.gz`);
    await uploadConfirmedAsset(repository, archive);
  }
  return replayOutput;
}

async function main(argv = process.argv.slice(2)): Promise<void> {
  const { command, values } = parseArguments(argv);
  const controller = new AbortController(), stop = () => controller.abort();
  process.on('SIGINT', stop); process.on('SIGTERM', stop);
  try {
    if (command === 'prepare-block') {
      const block = required(values, '--block');
      const preparation = planStudyPreparation(block as StudyBlock, Date.now());
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
      const directory = await smoke(path.resolve(required(values, '--workspace')), values.get('--repo'));
      output('action', 'SMOKE_COMPLETE'); output('report_path', directory); console.log(directory); return;
    }
    const repository = required(values, '--repo');
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
      const result = await captureStudyBlock(repository, block as StudyBlock, path.resolve(required(values, '--workspace')),
        required(values, '--run-id'), Number(required(values, '--run-attempt')), controller.signal);
      output('action', result.action); if (result.reportPath) output('report_path', result.reportPath);
      if (process.env.GITHUB_STEP_SUMMARY) {
        const summary = result.action === 'SKIPPED'
          ? `### Сбор пропущен\n\nПричина: \`${result.reason}\`. Эта задача не создала новую попытку записи. Успех служебного запуска не означает собранный торговый день.\n`
          : `### Блок записан\n\nПодтверждено новых частей: ${result.chunks}. Полнота дня оценивается отдельной проверкой данных.\n`;
        writeFileSync(process.env.GITHUB_STEP_SUMMARY, summary, { flag: 'a' });
      }
      console.log(JSON.stringify(result)); return;
    }
    throw new Error('Expected prepare-block, preflight, smoke, run-block, freeze, or status command');
  } finally {
    controller.abort(); process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(() => { console.error('Market study command failed; inspect the workflow summary and immutable archive.'); process.exitCode = 1; });
}
