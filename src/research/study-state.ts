import {
  STUDY_60_ATTEMPT_PROTOCOL_HASH, STUDY_100_ATTEMPT_PROTOCOL_HASH, STUDY_MAX_ATTEMPTS, STUDY_PROTOCOL_HASH, STUDY_REQUIRED_DAYS,
  STUDY_TICKERS, studyProtocol, hashStudyValue, type StudyBlock,
} from './study-protocol.js';
import { expectedStudyDayChunks, selectStudyDayInputs, studyDayInputHash } from './study-day-inputs.js';

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
  thresholds?: { timer: number; perInstrument: number };
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
  attemptBudgetMigration?: StudyAttemptBudgetMigration;
  attemptBudgetExtension?: StudyAttemptBudgetExtension;
}

/** Durable proof of the one supported predecessor. Historical receipts keep their original hashes. */
export interface StudyAttemptBudgetMigration {
  fromProtocolHash: typeof STUDY_60_ATTEMPT_PROTOCOL_HASH;
  toProtocolHash: typeof STUDY_100_ATTEMPT_PROTOCOL_HASH;
  sourceLedgerHash: string;
  migratedAt: string;
  legacyAttemptCount: 60;
  legacyChunkCount: number;
  legacyAttemptIdentityHash: string;
  legacyChunksHash: string;
  legacyCanonicalKeys: string[];
  legacyCanonicalsHash: string;
}

/** Pins every identity present when the 100-attempt ledger is explicitly extended. */
export interface StudyAttemptBudgetExtension {
  fromProtocolHash: typeof STUDY_100_ATTEMPT_PROTOCOL_HASH;
  toProtocolHash: typeof STUDY_PROTOCOL_HASH;
  fromLimit: 100;
  toLimit: 120;
  qualityChange: { fromDailyTimerCoverage: 0.99; toDailyTimerCoverage: 0.8; perInstrumentCoverage: 0.8 };
  sourceLedgerHash: string;
  migratedAt: string;
  oldAttemptCount: number;
  oldAttemptIdentityHash: string;
  oldChunkCount: number;
  oldChunksHash: string;
  oldCanonicalKeys: string[];
  oldCanonicalsHash: string;
  oldDayCount: number;
  oldDaysHash: string;
  originalMigrationHash: string;
}

/** An immutable uploaded report can lose an optimistic race to a newer
 * quality-selected input. It remains an archived diagnostic, not a day. */
export class StaleStudyDayInputError extends Error {}

const hash = /^[0-9a-f]{64}$/;
const digest = /^(?:sha256:)?[0-9a-f]{64}$/;
const isoDate = /^\d{4}-\d{2}-\d{2}$/;

export function createStudyLedger(now = new Date().toISOString()): StudyLedger {
  return { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, phase: 'DEVELOPMENT', attempts: [],
    chunks: [], canonicalChunks: {}, days: [], freeze: null, updatedAt: now };
}

function attemptIdentity(attempt: StudyAttempt): Omit<StudyAttempt, 'status'> {
  const { status: _status, ...identity } = attempt;
  return identity;
}

export function assertStudyLedgerProtocol(ledger: StudyLedger): void {
  if (ledger.schemaVersion !== 1 || ledger.protocolHash !== STUDY_PROTOCOL_HASH) throw new Error('Study ledger protocol mismatch');
  const extension = ledger.attemptBudgetExtension;
  if (!extension) {
    if (ledger.attemptBudgetMigration) throw new Error('Study ledger has no attempt-budget extension receipt');
    if (ledger.chunks.some(chunk => chunk.protocolHash !== STUDY_PROTOCOL_HASH)
      || ledger.days.some(day => day.protocolHash !== STUDY_PROTOCOL_HASH)) throw new Error('Study ledger contains foreign receipts');
    return;
  }
  assert100AttemptHistory(ledger, extension);
  if (extension.fromProtocolHash !== STUDY_100_ATTEMPT_PROTOCOL_HASH || extension.toProtocolHash !== STUDY_PROTOCOL_HASH
    || extension.fromLimit !== 100 || extension.toLimit !== 120
    || !extension.qualityChange || hashStudyValue(extension.qualityChange) !== hashStudyValue({ fromDailyTimerCoverage: .99,
      toDailyTimerCoverage: .8, perInstrumentCoverage: .8 })
    || !hash.test(extension.sourceLedgerHash) || !Number.isFinite(Date.parse(extension.migratedAt))
    || !Number.isSafeInteger(extension.oldAttemptCount) || extension.oldAttemptCount < 0 || extension.oldAttemptCount > 100
    || !Number.isSafeInteger(extension.oldChunkCount) || extension.oldChunkCount < 0
    || !Number.isSafeInteger(extension.oldDayCount) || extension.oldDayCount < 0
    || !hash.test(extension.oldAttemptIdentityHash) || !hash.test(extension.oldChunksHash)
    || !hash.test(extension.oldCanonicalsHash) || !hash.test(extension.oldDaysHash)
    || !hash.test(extension.originalMigrationHash)
    || !Array.isArray(extension.oldCanonicalKeys)
    || new Set(extension.oldCanonicalKeys).size !== extension.oldCanonicalKeys.length
    || extension.oldCanonicalKeys.some(key => typeof key !== 'string')
    || ledger.attempts.length < extension.oldAttemptCount || ledger.chunks.length < extension.oldChunkCount
    || ledger.days.length < extension.oldDayCount
    || hashStudyValue(ledger.attempts.slice(0, extension.oldAttemptCount).map(attemptIdentity)) !== extension.oldAttemptIdentityHash
    || hashStudyValue(ledger.chunks.slice(0, extension.oldChunkCount)) !== extension.oldChunksHash
    || extension.oldCanonicalKeys.some(key => !Object.hasOwn(ledger.canonicalChunks, key))
    || hashStudyValue(Object.fromEntries(extension.oldCanonicalKeys.map(key => [key, ledger.canonicalChunks[key]])))
      !== extension.oldCanonicalsHash
    || hashStudyValue(ledger.days.slice(0, extension.oldDayCount)) !== extension.oldDaysHash
    || hashStudyValue(ledger.attemptBudgetMigration) !== extension.originalMigrationHash
    || ledger.chunks.slice(extension.oldChunkCount).some(chunk => chunk.protocolHash !== STUDY_PROTOCOL_HASH)
    || ledger.days.slice(extension.oldDayCount).some(day => day.protocolHash !== STUDY_PROTOCOL_HASH)
    || ledger.attempts.length > STUDY_MAX_ATTEMPTS) {
    throw new Error('Study attempt-budget extension receipt or historical data changed');
  }
}

function assert100AttemptHistory(ledger: StudyLedger, extension?: StudyAttemptBudgetExtension): void {
  const migration = ledger.attemptBudgetMigration;
  if (!migration || migration.fromProtocolHash !== STUDY_60_ATTEMPT_PROTOCOL_HASH
    || migration.toProtocolHash !== STUDY_100_ATTEMPT_PROTOCOL_HASH
    || !hash.test(migration.sourceLedgerHash) || !Number.isFinite(Date.parse(migration.migratedAt))
    || migration.legacyAttemptCount !== 60 || !Number.isSafeInteger(migration.legacyChunkCount)
    || migration.legacyChunkCount < 0 || !hash.test(migration.legacyAttemptIdentityHash)
    || !hash.test(migration.legacyChunksHash) || !Array.isArray(migration.legacyCanonicalKeys)
    || new Set(migration.legacyCanonicalKeys).size !== migration.legacyCanonicalKeys.length
    || migration.legacyCanonicalKeys.some(key => typeof key !== 'string')
    || !hash.test(migration.legacyCanonicalsHash)
    || ledger.attempts.length < migration.legacyAttemptCount || ledger.chunks.length < migration.legacyChunkCount
    || hashStudyValue(ledger.attempts.slice(0, migration.legacyAttemptCount).map(attemptIdentity)) !== migration.legacyAttemptIdentityHash
    || hashStudyValue(ledger.chunks.slice(0, migration.legacyChunkCount)) !== migration.legacyChunksHash
    || migration.legacyCanonicalKeys.some(key => !Object.hasOwn(ledger.canonicalChunks, key))
    || hashStudyValue(Object.fromEntries(migration.legacyCanonicalKeys.map(key => [key, ledger.canonicalChunks[key]])))
      !== migration.legacyCanonicalsHash
    || ledger.chunks.slice(migration.legacyChunkCount, extension?.oldChunkCount).some(chunk => chunk.protocolHash !== STUDY_100_ATTEMPT_PROTOCOL_HASH)
    || ledger.days.slice(0, extension?.oldDayCount).some(day => day.protocolHash !== STUDY_100_ATTEMPT_PROTOCOL_HASH)) {
    throw new Error('Study attempt-budget migration receipt or historical data changed');
  }
}

/** Explicit, one-time transition; a normal writer must never upgrade a ledger on read. */
export function migrateStudyAttemptBudget(ledger: StudyLedger, migratedAt: string): StudyLedger {
  if (ledger.protocolHash === STUDY_100_ATTEMPT_PROTOCOL_HASH) {
    assert100AttemptHistory(ledger);
    return structuredClone(ledger);
  }
  if (ledger.schemaVersion !== 1 || ledger.protocolHash !== STUDY_60_ATTEMPT_PROTOCOL_HASH
    || ledger.attemptBudgetMigration || ledger.phase !== 'DEVELOPMENT' || ledger.freeze !== null || ledger.days.length !== 0
    || ledger.attempts.length !== 60 || ledger.attempts.some(attempt => attempt.mode !== 'COUNTED')
    || ledger.chunks.some(chunk => chunk.protocolHash !== STUDY_60_ATTEMPT_PROTOCOL_HASH)
    || !Number.isFinite(Date.parse(migratedAt))) throw new Error('Study ledger is not the authorized 60-attempt predecessor');
  const ids = new Set(ledger.attempts.map(attempt => attempt.attemptId));
  const assets = new Set(ledger.chunks.map(chunk => chunk.assetId));
  if (ids.size !== 60 || assets.size !== ledger.chunks.length
    || ledger.chunks.some(chunk => !ids.has(chunk.attemptId))
    || Object.entries(ledger.canonicalChunks).some(([key, assetId]) =>
      !ledger.chunks.some(chunk => chunk.assetId === assetId && chunk.chunkId === key && chunk.quality === 'PASS'))) {
    throw new Error('Legacy study ledger has inconsistent attempt or canonical identity');
  }
  const next = structuredClone(ledger);
  next.protocolHash = STUDY_100_ATTEMPT_PROTOCOL_HASH;
  next.attemptBudgetMigration = {
    fromProtocolHash: STUDY_60_ATTEMPT_PROTOCOL_HASH, toProtocolHash: STUDY_100_ATTEMPT_PROTOCOL_HASH,
    sourceLedgerHash: hashStudyValue(ledger), migratedAt, legacyAttemptCount: 60,
    legacyChunkCount: ledger.chunks.length,
    legacyAttemptIdentityHash: hashStudyValue(ledger.attempts.map(attemptIdentity)),
    legacyChunksHash: hashStudyValue(ledger.chunks),
    legacyCanonicalKeys: Object.keys(ledger.canonicalChunks).sort(),
    legacyCanonicalsHash: hashStudyValue(ledger.canonicalChunks),
  };
  assert100AttemptHistory(next);
  return next;
}

/** Explicit joint 100/99-to-120/80 transition; historical receipts and migration stay unchanged. */
export function extendStudyAttemptBudget(ledger: StudyLedger, migratedAt: string): StudyLedger {
  if (ledger.protocolHash === STUDY_PROTOCOL_HASH) {
    assertStudyLedgerProtocol(ledger);
    if (!ledger.attemptBudgetExtension) throw new Error('Study ledger has no attempt-budget extension receipt');
    return structuredClone(ledger);
  }
  if (ledger.schemaVersion !== 1 || ledger.protocolHash !== STUDY_100_ATTEMPT_PROTOCOL_HASH
    || ledger.attemptBudgetExtension || ledger.phase !== 'DEVELOPMENT' || ledger.freeze !== null
    || ledger.attempts.length > 100 || ledger.days.some(day => day.quality.status === 'PASS')
    || !Number.isFinite(Date.parse(migratedAt))) {
    throw new Error('Study ledger is not the authorized 100-attempt predecessor');
  }
  assert100AttemptHistory(ledger);
  ledger.days.forEach(validateDay);
  const next = structuredClone(ledger);
  next.protocolHash = STUDY_PROTOCOL_HASH;
  next.attemptBudgetExtension = {
    fromProtocolHash: STUDY_100_ATTEMPT_PROTOCOL_HASH, toProtocolHash: STUDY_PROTOCOL_HASH,
    fromLimit: 100, toLimit: 120, sourceLedgerHash: hashStudyValue(ledger), migratedAt,
    qualityChange: { fromDailyTimerCoverage: .99,
      toDailyTimerCoverage: studyProtocol.quality.minimumDailyTimerCoverage,
      perInstrumentCoverage: studyProtocol.quality.minimumDailyPerInstrumentCoverage },
    oldAttemptCount: ledger.attempts.length,
    oldAttemptIdentityHash: hashStudyValue(ledger.attempts.map(attemptIdentity)),
    oldChunkCount: ledger.chunks.length, oldChunksHash: hashStudyValue(ledger.chunks),
    oldCanonicalKeys: Object.keys(ledger.canonicalChunks).sort(),
    oldCanonicalsHash: hashStudyValue(ledger.canonicalChunks),
    oldDayCount: ledger.days.length, oldDaysHash: hashStudyValue(ledger.days),
    originalMigrationHash: hashStudyValue(ledger.attemptBudgetMigration),
  };
  assertStudyLedgerProtocol(next);
  return next;
}

function clone(ledger: StudyLedger): StudyLedger {
  assertStudyLedgerProtocol(ledger);
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
  if (receipt.schemaVersion !== 1 || ![STUDY_PROTOCOL_HASH, STUDY_100_ATTEMPT_PROTOCOL_HASH, STUDY_60_ATTEMPT_PROTOCOL_HASH].includes(receipt.protocolHash)
    || !Number.isSafeInteger(receipt.assetId)
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
  if (receipt.protocolHash !== STUDY_PROTOCOL_HASH) throw new Error('New upload uses a retired study protocol');
  const finalized = next.days.some(day => day.sessionDate === receipt.sessionDate);
  const sameName = next.chunks.find((chunk) => chunk.assetName === receipt.assetName);
  if (sameName) throw new Error('Artifact name collision');
  const attempt = next.attempts.find((item) => item.attemptId === receipt.attemptId);
  if (!attempt || attempt.mode !== 'COUNTED' || attempt.phase !== receipt.phase
    || attempt.sessionDate !== receipt.sessionDate || attempt.block !== receipt.block) throw new Error('Chunk does not match its persisted attempt');
  if (next.attemptBudgetExtension && next.attempts.slice(0, next.attemptBudgetExtension.oldAttemptCount)
    .some(item => item.attemptId === receipt.attemptId)) throw new Error('Pre-extension attempt cannot create a new scientific receipt');
  if (receipt.phase === 'HOLDOUT' && (!next.freeze || receipt.replayConfigHash !== next.freeze.replayConfigHash
    || !sameHashes(receipt.simulatorHashes, next.freeze.simulatorHashes))) throw new Error('Holdout capture differs from the frozen implementation');
  next.chunks.push(structuredClone(receipt));
  attempt.status = 'UPLOADED';
  if (!finalized && receipt.quality === 'PASS' && next.canonicalChunks[receipt.chunkId] === undefined) {
    next.canonicalChunks[receipt.chunkId] = receipt.assetId;
  }
  next.updatedAt = [next.updatedAt, receipt.uploadedAt].sort().at(-1)!;
  return next;
}

export function dailyQualityThresholds(protocolHash: string): { timer: number; perInstrument: number } {
  if (protocolHash === STUDY_PROTOCOL_HASH) return {
    timer: studyProtocol.quality.minimumDailyTimerCoverage,
    perInstrument: studyProtocol.quality.minimumDailyPerInstrumentCoverage,
  };
  if (protocolHash === STUDY_100_ATTEMPT_PROTOCOL_HASH || protocolHash === STUDY_60_ATTEMPT_PROTOCOL_HASH) {
    return { timer: .99, perInstrument: .8 };
  }
  throw new Error('Unknown daily study protocol');
}

function validateDay(receipt: StudyDayReceipt): void {
  if (receipt.schemaVersion !== 1 || ![STUDY_PROTOCOL_HASH, STUDY_100_ATTEMPT_PROTOCOL_HASH, STUDY_60_ATTEMPT_PROTOCOL_HASH].includes(receipt.protocolHash) || !receipt.dayId
    || !receipt.sessionDate.match(isoDate) || !hash.test(receipt.canonicalInputHash) || !hash.test(receipt.replayConfigHash)
    || !receipt.chunkAssetIds.length || new Set(receipt.chunkAssetIds).size !== receipt.chunkAssetIds.length
    || !Number.isFinite(receipt.quality.recordedShare) || receipt.quality.recordedShare < 0 || receipt.quality.recordedShare > 1
    || !Number.isSafeInteger(receipt.quality.expectedTicks) || receipt.quality.expectedTicks <= 0
    || !Number.isSafeInteger(receipt.quality.observedTicks) || receipt.quality.observedTicks < 0
    || receipt.quality.observedTicks > receipt.quality.expectedTicks
    || receipt.dayId !== `${receipt.protocolHash}:${receipt.sessionDate}`
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
  const thresholds = dailyQualityThresholds(receipt.protocolHash);
  if (receipt.quality.thresholds && (receipt.quality.thresholds.timer !== thresholds.timer
    || receipt.quality.thresholds.perInstrument !== thresholds.perInstrument)) {
    throw new Error('Daily study quality thresholds contradict protocol');
  }
  const passes = receipt.quality.recordedShare >= thresholds.timer
    && instruments.every(item => item.usableShare >= thresholds.perInstrument);
  if ((receipt.quality.status === 'PASS') !== passes) throw new Error('Daily study quality status contradicts thresholds');
}
function passedDates(ledger: StudyLedger, phase: 'DEVELOPMENT' | 'HOLDOUT'): string[] {
  return [...new Set(ledger.days.filter((day) => day.phase === phase && day.quality.status === 'PASS').map((day) => day.sessionDate))].sort();
}
function updatePhase(ledger: StudyLedger): void {
  if (ledger.phase === 'DEVELOPMENT' && passedDates(ledger, 'DEVELOPMENT').length >= STUDY_REQUIRED_DAYS) ledger.phase = 'READY_TO_FREEZE';
  if (ledger.phase === 'HOLDOUT' && passedDates(ledger, 'HOLDOUT').length >= STUDY_REQUIRED_DAYS) ledger.phase = 'COMPLETE';
}

function assertDayInputs(ledger: StudyLedger, receipt: StudyDayReceipt): void {
  const expected = expectedStudyDayChunks(receipt);
  if (receipt.chunkAssetIds.length !== expected.length) {
    throw new Error('Full study day requires every planned receipt from one phase');
  }
  const selected = receipt.chunkAssetIds.map((assetId, index) => {
    const chunk = ledger.chunks.find(item => item.assetId === assetId);
    const planned = expected[index]!;
    if (!chunk || chunk.chunkId !== planned.key || chunk.sessionDate !== receipt.sessionDate
      || chunk.phase !== receipt.phase || chunk.plannedStart !== planned.chunk.plannedStart
      || chunk.plannedEnd !== planned.chunk.plannedEnd) {
      throw new Error('Daily receipt does not match every exact planned window and phase');
    }
    return chunk;
  });
  if (studyDayInputHash(selected) !== receipt.canonicalInputHash) {
    throw new Error('Daily receipt does not match the immutable full-day input selection');
  }
  if (selected.some(chunk => chunk.replayConfigHash !== receipt.replayConfigHash
    || !sameHashes(chunk.simulatorHashes, receipt.simulatorHashes))) {
    throw new Error('Daily replay differs from the capture-time implementation');
  }
}

function assertFreshDaySelection(ledger: StudyLedger, receipt: StudyDayReceipt): void {
  const selection = selectStudyDayInputs(ledger, receipt);
  if (selection.receipts.length !== selection.expectedCount || selection.phase !== receipt.phase) {
    throw new Error('Full study day requires every planned receipt from one phase');
  }
  if (JSON.stringify(selection.receipts.map(chunk => chunk.assetId)) !== JSON.stringify(receipt.chunkAssetIds)) {
    throw new StaleStudyDayInputError('Recovered day report is stale relative to the deterministic input selection');
  }
}

function pinDayCanonicals(ledger: StudyLedger, day: StudyDayReceipt): void {
  for (const assetId of day.chunkAssetIds) {
    const chunk = ledger.chunks.find(item => item.assetId === assetId)!;
    if (chunk.quality === 'PASS') ledger.canonicalChunks[chunk.chunkId] = assetId;
    else delete ledger.canonicalChunks[chunk.chunkId];
  }
}

export function acceptStudyDay(ledger: StudyLedger, receipt: StudyDayReceipt): StudyLedger {
  validateDay(receipt);
  const next = clone(ledger);
  const existing = next.days.find((day) => day.dayId === receipt.dayId);
  if (existing) {
    if (JSON.stringify(existing) !== JSON.stringify(receipt)) throw new Error('Finalized study day changed');
    return next;
  }
  if (receipt.protocolHash !== STUDY_PROTOCOL_HASH) throw new Error('New day report uses a retired study protocol');
  if (next.attemptBudgetExtension && next.attempts.slice(0, next.attemptBudgetExtension.oldAttemptCount)
    .some(attempt => attempt.sessionDate === receipt.sessionDate)) {
    throw new Error('Pre-extension study date cannot create a new scientific day');
  }
  if (receipt.phase !== phaseForCapture(next.phase)) throw new Error('Day phase does not match ledger phase');
  if (receipt.phase === 'HOLDOUT' && next.freeze?.developmentDates.includes(receipt.sessionDate)) throw new Error('Development day leaked into holdout');
  assertDayInputs(next, receipt);
  assertFreshDaySelection(next, receipt);
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
  pinDayCanonicals(next, receipt);
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
  if (JSON.stringify(merged.attemptBudgetMigration ?? null) !== JSON.stringify(incoming.attemptBudgetMigration ?? null)) {
    throw new Error('Conflicting study attempt-budget migrations');
  }
  if (JSON.stringify(merged.attemptBudgetExtension ?? null) !== JSON.stringify(incoming.attemptBudgetExtension ?? null)) {
    throw new Error('Conflicting study attempt-budget extensions');
  }
  const leftCanonicals = { ...merged.canonicalChunks };
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
  // A successful optimistic write on the remote branch precedes the stale
  // writer. Asset IDs are upload order, not ledger acceptance order.
  for (const mapping of [leftCanonicals, incoming.canonicalChunks]) {
    for (const [chunkId, assetId] of Object.entries(mapping)) {
      const chunk = merged.chunks.find(item => item.assetId === assetId);
      if (!chunk || chunk.chunkId !== chunkId || chunk.quality !== 'PASS') {
        throw new Error('Merged canonical mapping lacks its quality-passing receipt');
      }
      merged.canonicalChunks[chunkId] ??= assetId;
    }
  }
  for (const day of incoming.days) {
    const current = merged.days.find(item => item.dayId === day.dayId);
    if (current && JSON.stringify(current) !== JSON.stringify(day)) throw new Error('Conflicting finalized study days');
    if (!current) {
      assertDayInputs(merged, day);
      assertFreshDaySelection(merged, day);
      merged.days.push(structuredClone(day));
    }
  }
  for (const day of merged.days) pinDayCanonicals(merged, day);
  for (const day of merged.days) {
    validateDay(day);
    assertDayInputs(merged, day);
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
