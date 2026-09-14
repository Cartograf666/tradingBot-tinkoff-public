import {
  STUDY_MAX_ATTEMPTS, STUDY_PROTOCOL_HASH, STUDY_REQUIRED_DAYS, STUDY_TICKERS, type StudyBlock,
} from './study-protocol.js';

export type StudyPhase = 'DEVELOPMENT' | 'READY_TO_FREEZE' | 'HOLDOUT' | 'COMPLETE';
export type StudyRunMode = 'COUNTED' | 'SMOKE';
export interface StudyAttempt {
  attemptId: string;
  runId: string;
  runAttempt: number;
  sessionDate: string | null;
  block: StudyBlock | null;
  phase: 'DEVELOPMENT' | 'HOLDOUT' | null;
  mode: StudyRunMode;
  startedAt: string;
  status: 'STARTED' | 'UPLOADED' | 'FAILED';
}
export interface StudyChunkReceipt {
  schemaVersion: 1;
  protocolHash: string;
  assetId: number;
  assetName: string;
  assetDigest: string;
  assetBytes: number;
  archiveSha256: string;
  releaseTag: string;
  attemptId: string;
  chunkId: string;
  sessionDate: string;
  phase: 'DEVELOPMENT' | 'HOLDOUT';
  block: StudyBlock;
  chunkIndex: number;
  plannedStart: string;
  plannedEnd: string;
  runId: string;
  manifestHash: string;
  recordingHash: string;
  replayConfigHash: string;
  simulatorHashes: Record<string, string>;
  quality: 'PASS' | 'INSUFFICIENT_DATA';
  uploadedAt: string;
}
export interface DailyStudyQuality {
  status: 'PASS' | 'INSUFFICIENT_DATA';
  recordedShare: number;
  expectedTicks: number;
  observedTicks: number;
  perInstrument: Array<{ ticker: string; usableShare: number }>;
}
export interface StudyDayReceipt {
  schemaVersion: 1;
  protocolHash: string;
  dayId: string;
  sessionDate: string;
  phase: 'DEVELOPMENT' | 'HOLDOUT';
  mainStart: string;
  mainEnd: string;
  chunkAssetIds: number[];
  canonicalInputHash: string;
  replayConfigHash: string;
  simulatorHashes: Record<string, string>;
  reportAssetId: number;
  reportAssetName: string;
  reportAssetDigest: string;
  reportAssetBytes: number;
  reportArchiveSha256: string;
  quality: DailyStudyQuality;
  results: unknown[];
  finalizedAt: string;
}
export interface StudyFreeze {
  frozenAt: string;
  holdoutNotBeforeDate: string;
  protocolHash: string;
  replayConfigHash: string;
  simulatorHashes: Record<string, string>;
  developmentDates: string[];
}
export interface StudyLedger {
  schemaVersion: 1;
  protocolHash: string;
  phase: StudyPhase;
  attempts: StudyAttempt[];
  chunks: StudyChunkReceipt[];
  canonicalChunks: Record<string, number>;
  days: StudyDayReceipt[];
  freeze: StudyFreeze | null;
  updatedAt: string;
}

const hash = /^[0-9a-f]{64}$/;
const digest = /^(?:sha256:)?[0-9a-f]{64}$/;
const isoDate = /^\d{4}-\d{2}-\d{2}$/;

export function createStudyLedger(now = new Date().toISOString()): StudyLedger {
  return { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, phase: 'DEVELOPMENT', attempts: [],
    chunks: [], canonicalChunks: {}, days: [], freeze: null, updatedAt: now };
}

function clone(ledger: StudyLedger): StudyLedger {
  if (ledger.schemaVersion !== 1 || ledger.protocolHash !== STUDY_PROTOCOL_HASH) throw new Error('Study ledger protocol mismatch');
  return structuredClone(ledger);
}
function phaseForCapture(phase: StudyPhase): 'DEVELOPMENT' | 'HOLDOUT' | null {
  return phase === 'DEVELOPMENT' || phase === 'HOLDOUT' ? phase : null;
}
function moscowDate(instant: string): string {
  const value = Date.parse(instant);
  if (!Number.isFinite(value)) throw new Error('Invalid study timestamp');
  return new Date(value + 3 * 3_600_000).toISOString().slice(0, 10);
}
function nextDate(date: string): string {
  const value = Date.parse(`${date}T00:00:00.000Z`);
  if (!Number.isFinite(value)) throw new Error('Invalid study date');
  return new Date(value + 86_400_000).toISOString().slice(0, 10);
}
function phaseForDate(ledger: StudyLedger, sessionDate: string): 'DEVELOPMENT' | 'HOLDOUT' | null {
  const phases = new Set(ledger.attempts.filter(attempt => attempt.mode === 'COUNTED'
    && attempt.sessionDate === sessionDate && attempt.phase).map(attempt => attempt.phase!));
  if (phases.size > 1) throw new Error('Study date spans multiple phases');
  return phases.values().next().value ?? phaseForCapture(ledger.phase);
}
function attemptKey(runId: string, runAttempt: number): string {
  if (!runId || !Number.isSafeInteger(runAttempt) || runAttempt <= 0) throw new Error('Invalid workflow attempt identity');
  return `${runId}:${runAttempt}`;
}

export type StudyPlan =
  | { action: 'CAPTURE'; mode: StudyRunMode; phase: 'DEVELOPMENT' | 'HOLDOUT' | null; attemptId: string }
  | { action: 'PAUSED_CONFIGURATION' | 'PAUSED_PHASE' | 'COMPLETE' | 'ATTEMPT_LIMIT'; reason: string };

export function planStudyRun(
  ledger: StudyLedger,
  input: { event: 'schedule' | 'manual_capture' | 'smoke'; campaignEnabled: boolean; runId: string; runAttempt: number; sessionDate?: string },
): StudyPlan {
  clone(ledger);
  if (input.event === 'smoke') return { action: 'CAPTURE', mode: 'SMOKE', phase: null, attemptId: attemptKey(input.runId, input.runAttempt) };
  if (!input.campaignEnabled) return { action: 'PAUSED_CONFIGURATION', reason: 'Full-session campaign awaits storage credentials, sandbox smoke and activation' };
  if (ledger.phase === 'COMPLETE') return { action: 'COMPLETE', reason: 'Both study phases are complete' };
  const phase = input.sessionDate ? phaseForDate(ledger, input.sessionDate) : phaseForCapture(ledger.phase);
  if (!phase) return { action: 'PAUSED_PHASE', reason: 'Development is complete; freeze is required before holdout' };
  if (ledger.attempts.length >= STUDY_MAX_ATTEMPTS) return { action: 'ATTEMPT_LIMIT', reason: 'Attempted collection-job limit reached' };
  return { action: 'CAPTURE', mode: 'COUNTED', phase, attemptId: attemptKey(input.runId, input.runAttempt) };
}

export function beginStudyAttempt(
  ledger: StudyLedger,
  input: { runId: string; runAttempt: number; sessionDate: string | null; block: StudyBlock | null; mode: StudyRunMode; startedAt: string },
): StudyLedger {
  const next = clone(ledger), key = attemptKey(input.runId, input.runAttempt);
  if (next.attempts.some((attempt) => attempt.attemptId === key)) return next;
  if (next.attempts.length >= STUDY_MAX_ATTEMPTS) throw new Error('Study attempt limit reached');
  if (input.mode === 'COUNTED' && (!input.sessionDate?.match(isoDate) || !input.block)) throw new Error('Counted attempt requires a study date and block');
  const phase = input.mode === 'COUNTED' ? phaseForDate(next, input.sessionDate!) : null;
  if (input.mode === 'COUNTED' && !phase) throw new Error('Current study phase does not accept capture');
  if (input.mode === 'COUNTED' && next.freeze?.developmentDates.includes(input.sessionDate!)) {
    throw new Error('A development date cannot become holdout');
  }
  if (phase === 'HOLDOUT' && (!next.freeze || input.sessionDate! < next.freeze.holdoutNotBeforeDate
    || next.freeze.developmentDates.includes(input.sessionDate!))) {
    throw new Error('A development date cannot become holdout');
  }
  next.attempts.push({ attemptId: key, runId: input.runId, runAttempt: input.runAttempt,
    sessionDate: input.sessionDate, block: input.block, phase, mode: input.mode, startedAt: input.startedAt, status: 'STARTED' });
  next.updatedAt = input.startedAt;
  return next;
}

function validateChunk(receipt: StudyChunkReceipt): void {
  if (receipt.schemaVersion !== 1 || receipt.protocolHash !== STUDY_PROTOCOL_HASH || !Number.isSafeInteger(receipt.assetId)
    || receipt.assetId <= 0 || !receipt.assetName || !digest.test(receipt.assetDigest) || !hash.test(receipt.archiveSha256)
    || !Number.isSafeInteger(receipt.assetBytes) || receipt.assetBytes <= 0 || !receipt.chunkId || !receipt.attemptId
    || !receipt.sessionDate.match(isoDate) || !['early', 'late'].includes(receipt.block)
    || !Number.isSafeInteger(receipt.chunkIndex) || receipt.chunkIndex <= 0 || !hash.test(receipt.manifestHash)
    || !hash.test(receipt.recordingHash) || !hash.test(receipt.replayConfigHash)
    || !Object.keys(receipt.simulatorHashes).length || Object.values(receipt.simulatorHashes).some(value => !hash.test(value))
    || !['DEVELOPMENT', 'HOLDOUT'].includes(receipt.phase) || !['PASS', 'INSUFFICIENT_DATA'].includes(receipt.quality)
    || receipt.releaseTag !== 'market-study-archive-v1' || !receipt.runId || !Number.isFinite(Date.parse(receipt.uploadedAt))
    || !(Date.parse(receipt.plannedEnd) > Date.parse(receipt.plannedStart))) {
    throw new Error('Uploaded chunk receipt is invalid');
  }
}

/** Merge only immutable, remotely confirmed release-asset receipts. PnL never participates in canonical selection. */
export function acceptStudyChunk(ledger: StudyLedger, receipt: StudyChunkReceipt): StudyLedger {
  validateChunk(receipt);
  const next = clone(ledger);
  const sameAsset = next.chunks.find((chunk) => chunk.assetId === receipt.assetId);
  if (sameAsset) {
    if (JSON.stringify(sameAsset) !== JSON.stringify(receipt)) throw new Error('Artifact identity changed');
    return next;
  }
  const sameName = next.chunks.find((chunk) => chunk.assetName === receipt.assetName);
  if (sameName) throw new Error('Artifact name collision');
  const attempt = next.attempts.find((item) => item.attemptId === receipt.attemptId);
  if (!attempt || attempt.mode !== 'COUNTED' || attempt.phase !== receipt.phase
    || attempt.sessionDate !== receipt.sessionDate || attempt.block !== receipt.block) throw new Error('Chunk does not match its persisted attempt');
  if (receipt.phase === 'HOLDOUT' && (!next.freeze || receipt.replayConfigHash !== next.freeze.replayConfigHash
    || !sameHashes(receipt.simulatorHashes, next.freeze.simulatorHashes))) throw new Error('Holdout capture differs from the frozen implementation');
  next.chunks.push(structuredClone(receipt));
  attempt.status = 'UPLOADED';
  if (receipt.quality === 'PASS' && next.canonicalChunks[receipt.chunkId] === undefined) {
    next.canonicalChunks[receipt.chunkId] = receipt.assetId;
  }
  next.updatedAt = receipt.uploadedAt;
  return next;
}

function validateDay(receipt: StudyDayReceipt): void {
  if (receipt.schemaVersion !== 1 || receipt.protocolHash !== STUDY_PROTOCOL_HASH || !receipt.dayId
    || !receipt.sessionDate.match(isoDate) || !hash.test(receipt.canonicalInputHash) || !hash.test(receipt.replayConfigHash)
    || !receipt.chunkAssetIds.length || new Set(receipt.chunkAssetIds).size !== receipt.chunkAssetIds.length
    || !Number.isFinite(receipt.quality.recordedShare) || receipt.quality.recordedShare < 0 || receipt.quality.recordedShare > 1
    || !Number.isSafeInteger(receipt.quality.expectedTicks) || receipt.quality.expectedTicks <= 0
    || !Number.isSafeInteger(receipt.quality.observedTicks) || receipt.quality.observedTicks < 0
    || receipt.quality.observedTicks > receipt.quality.expectedTicks
    || receipt.dayId !== `${STUDY_PROTOCOL_HASH}:${receipt.sessionDate}`
    || !Number.isSafeInteger(receipt.reportAssetId) || receipt.reportAssetId <= 0 || !receipt.reportAssetName
    || !digest.test(receipt.reportAssetDigest) || !Number.isSafeInteger(receipt.reportAssetBytes) || receipt.reportAssetBytes <= 0
    || !hash.test(receipt.reportArchiveSha256) || Object.keys(receipt.simulatorHashes).length === 0
    || Object.values(receipt.simulatorHashes).some(value => !hash.test(value))) throw new Error('Daily study receipt is invalid');
  const instruments = receipt.quality.perInstrument;
  if (instruments.length !== STUDY_TICKERS.length
    || new Set(instruments.map(item => item.ticker)).size !== STUDY_TICKERS.length
    || STUDY_TICKERS.some(ticker => !instruments.some(item => item.ticker === ticker))
    || instruments.some(item => !Number.isFinite(item.usableShare) || item.usableShare < 0 || item.usableShare > 1)) {
    throw new Error('Daily study instrument quality is invalid');
  }
  const passes = receipt.quality.recordedShare >= 0.99 && instruments.every(item => item.usableShare >= 0.8);
  if ((receipt.quality.status === 'PASS') !== passes) throw new Error('Daily study quality status contradicts thresholds');
}
function passedDates(ledger: StudyLedger, phase: 'DEVELOPMENT' | 'HOLDOUT'): string[] {
  return [...new Set(ledger.days.filter((day) => day.phase === phase && day.quality.status === 'PASS').map((day) => day.sessionDate))].sort();
}
function updatePhase(ledger: StudyLedger): void {
  if (ledger.phase === 'DEVELOPMENT' && passedDates(ledger, 'DEVELOPMENT').length >= STUDY_REQUIRED_DAYS) ledger.phase = 'READY_TO_FREEZE';
  if (ledger.phase === 'HOLDOUT' && passedDates(ledger, 'HOLDOUT').length >= STUDY_REQUIRED_DAYS) ledger.phase = 'COMPLETE';
}

export function acceptStudyDay(ledger: StudyLedger, receipt: StudyDayReceipt): StudyLedger {
  validateDay(receipt);
  const next = clone(ledger);
  const existing = next.days.find((day) => day.dayId === receipt.dayId);
  if (existing) {
    if (JSON.stringify(existing) !== JSON.stringify(receipt)) throw new Error('Finalized study day changed');
    return next;
  }
  if (receipt.phase !== phaseForCapture(next.phase)) throw new Error('Day phase does not match ledger phase');
  if (receipt.phase === 'HOLDOUT' && next.freeze?.developmentDates.includes(receipt.sessionDate)) throw new Error('Development day leaked into holdout');
  const chunks = receipt.chunkAssetIds.map((id) => next.chunks.find((chunk) => chunk.assetId === id));
  if (chunks.some((chunk) => !chunk)) throw new Error('Daily receipt references an unknown release asset');
  if (!chunks.some((chunk) => chunk!.block === 'early') || !chunks.some((chunk) => chunk!.block === 'late')) {
    throw new Error('Full study day requires both ownership blocks');
  }
  if (chunks.some((chunk) => chunk!.sessionDate !== receipt.sessionDate || chunk!.phase !== receipt.phase
    || next.canonicalChunks[chunk!.chunkId] !== chunk!.assetId)) throw new Error('Daily receipt must use canonical chunks from one phase and date');
  if (chunks.some(chunk => chunk!.replayConfigHash !== receipt.replayConfigHash
    || !sameHashes(chunk!.simulatorHashes, receipt.simulatorHashes))) {
    throw new Error('Daily replay differs from the capture-time implementation');
  }
  const allCanonical = next.chunks.filter(chunk => chunk.sessionDate === receipt.sessionDate
    && chunk.phase === receipt.phase && next.canonicalChunks[chunk.chunkId] === chunk.assetId).map(chunk => chunk.assetId).sort((a, b) => a - b);
  if (JSON.stringify([...receipt.chunkAssetIds].sort((a, b) => a - b)) !== JSON.stringify(allCanonical)) {
    throw new Error('Daily receipt omits a canonical chunk');
  }
  if (receipt.phase === 'HOLDOUT' && (!next.freeze || receipt.replayConfigHash !== next.freeze.replayConfigHash
    || !sameHashes(receipt.simulatorHashes, next.freeze.simulatorHashes))) {
    throw new Error('Holdout replay differs from the frozen implementation');
  }
  const developmentIdentity = next.days.find(day => day.phase === 'DEVELOPMENT' && day.quality.status === 'PASS');
  if (receipt.phase === 'DEVELOPMENT' && receipt.quality.status === 'PASS' && developmentIdentity
    && (receipt.replayConfigHash !== developmentIdentity.replayConfigHash
      || !sameHashes(receipt.simulatorHashes, developmentIdentity.simulatorHashes))) {
    throw new Error('Passing development days must use one replay implementation');
  }
  next.days.push(structuredClone(receipt));
  next.updatedAt = receipt.finalizedAt;
  updatePhase(next);
  return next;
}

function sameHashes(left: Record<string, string>, right: Record<string, string>): boolean {
  const entries = (value: Record<string, string>) => Object.entries(value).sort(([a], [b]) => a.localeCompare(b));
  return JSON.stringify(entries(left)) === JSON.stringify(entries(right));
}

export function freezeStudy(
  ledger: StudyLedger,
  input: { frozenAt: string; replayConfigHash: string; simulatorHashes: Record<string, string> },
): StudyLedger {
  const next = clone(ledger);
  if (next.phase !== 'READY_TO_FREEZE' || next.freeze) throw new Error('Study is not ready to freeze');
  if (!hash.test(input.replayConfigHash) || !Object.keys(input.simulatorHashes).length
    || Object.values(input.simulatorHashes).some((value) => !hash.test(value))) throw new Error('Freeze source hashes are invalid');
  const dates = passedDates(next, 'DEVELOPMENT');
  if (dates.length < STUDY_REQUIRED_DAYS) throw new Error('Development days are incomplete');
  const developmentIdentity = next.days.find(day => day.phase === 'DEVELOPMENT' && day.quality.status === 'PASS');
  if (!developmentIdentity || input.replayConfigHash !== developmentIdentity.replayConfigHash
    || !sameHashes(input.simulatorHashes, developmentIdentity.simulatorHashes)) {
    throw new Error('Freeze must match the tested development implementation');
  }
  const developmentDates = [...new Set(next.attempts.filter(attempt => attempt.mode === 'COUNTED'
    && attempt.phase === 'DEVELOPMENT' && attempt.sessionDate).map(attempt => attempt.sessionDate!))].sort();
  const frozenDate = moscowDate(input.frozenAt);
  const holdoutNotBeforeDate = nextDate(frozenDate);
  if (developmentDates.some(date => date >= holdoutNotBeforeDate)) throw new Error('Freeze must precede a future Moscow study date');
  next.freeze = { frozenAt: input.frozenAt, protocolHash: STUDY_PROTOCOL_HASH,
    holdoutNotBeforeDate, replayConfigHash: input.replayConfigHash,
    simulatorHashes: { ...input.simulatorHashes }, developmentDates };
  next.phase = 'HOLDOUT'; next.updatedAt = input.frozenAt;
  return next;
}

/** Union receipts from two optimistic-SHA writers; immutable collisions fail instead of overwriting. */
export function mergeStudyLedgers(left: StudyLedger, right: StudyLedger): StudyLedger {
  const merged = clone(left), incoming = clone(right);
  if (merged.freeze && incoming.freeze && JSON.stringify(merged.freeze) !== JSON.stringify(incoming.freeze)) throw new Error('Conflicting study freezes');
  merged.freeze ??= incoming.freeze;
  for (const attempt of incoming.attempts) {
    const current = merged.attempts.find((item) => item.attemptId === attempt.attemptId);
    if (!current) merged.attempts.push(structuredClone(attempt));
    else {
      if (Object.entries(current).some(([key, value]) => key !== 'status'
        && JSON.stringify(value) !== JSON.stringify(attempt[key as keyof StudyAttempt]))) throw new Error('Conflicting study attempts');
      const rank = { STARTED: 0, FAILED: 1, UPLOADED: 2 } as const;
      if (rank[attempt.status] > rank[current.status]) current.status = attempt.status;
    }
  }
  if (merged.attempts.length > STUDY_MAX_ATTEMPTS) throw new Error('Concurrent study attempts exceed the campaign limit');
  for (const chunk of incoming.chunks) Object.assign(merged, acceptStudyChunk(merged, chunk));
  merged.canonicalChunks = {};
  for (const chunk of [...merged.chunks].sort((a, b) => a.assetId - b.assetId)) {
    if (chunk.quality === 'PASS' && merged.canonicalChunks[chunk.chunkId] === undefined) merged.canonicalChunks[chunk.chunkId] = chunk.assetId;
  }
  for (const day of incoming.days) {
    const current = merged.days.find(item => item.dayId === day.dayId);
    if (current && JSON.stringify(current) !== JSON.stringify(day)) throw new Error('Conflicting finalized study days');
    if (!current) merged.days.push(structuredClone(day));
  }
  for (const day of merged.days) {
    validateDay(day);
    for (const assetId of day.chunkAssetIds) {
      const chunk = merged.chunks.find(item => item.assetId === assetId);
      if (!chunk || merged.canonicalChunks[chunk.chunkId] !== assetId) throw new Error('Merge would shift a finalized canonical chunk');
    }
  }
  const developmentDays = merged.days.filter(day => day.phase === 'DEVELOPMENT' && day.quality.status === 'PASS');
  if (developmentDays.some(day => day.replayConfigHash !== developmentDays[0]?.replayConfigHash
    || !sameHashes(day.simulatorHashes, developmentDays[0]!.simulatorHashes))) {
    throw new Error('Merged development days use conflicting replay implementations');
  }
  if (merged.freeze) {
    const attemptedDates = [...new Set(merged.attempts.filter(attempt => attempt.mode === 'COUNTED'
      && attempt.phase === 'DEVELOPMENT' && attempt.sessionDate).map(attempt => attempt.sessionDate!))].sort();
    if (JSON.stringify(attemptedDates) !== JSON.stringify(merged.freeze.developmentDates)) {
      throw new Error('Freeze omitted a concurrent development attempt');
    }
  }
  merged.phase = merged.freeze ? 'HOLDOUT' : 'DEVELOPMENT';
  updatePhase(merged);
  merged.updatedAt = [left.updatedAt, right.updatedAt].sort().at(-1)!;
  return merged;
}
