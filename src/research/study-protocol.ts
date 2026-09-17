import { createHash } from 'node:crypto';

export const STUDY_PROTOCOL_VERSION = 1 as const;
export const STUDY_STATE_BRANCH = 'observation-state';
export const STUDY_RELEASE_TAG = 'market-study-archive-v1';
export const STUDY_REQUIRED_DAYS = 10;
export const STUDY_MAX_ATTEMPTS = 60;
export const STUDY_JOB_MAX_MS = 350 * 60 * 1_000;
export const STUDY_LAUNCH_LATENESS_MS = 20 * 60 * 1_000;
export const STUDY_CHUNK_MAX_MS = 30 * 60 * 1_000;
export const STUDY_CHUNK_END_MARGIN_MS = 5_000;
export const STUDY_TICKERS = ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'] as const;
export type StudyBlock = 'early' | 'late';

export const studyProtocol = {
  schemaVersion: STUDY_PROTOCOL_VERSION,
  campaignEnabledByDefault: false,
  campaignPauseReason: 'AWAITING_STORAGE_CREDENTIALS_AND_SANDBOX_SMOKE',
  source: 'exchange', session: 'main', tickers: STUDY_TICKERS, depth: 20,
  budgetRub: 4_000, commissionRate: 0.0005,
  smokeDurationSeconds: 60,
  automaticBlockCheck: { session: 'main', requiredBeforeChunks: true, failure: 'abort-block', includedInReplay: false },
  chunkMaxSeconds: STUDY_CHUNK_MAX_MS / 1_000,
  chunkEndMarginSeconds: STUDY_CHUNK_END_MARGIN_MS / 1_000,
  segmentMaxBytes: 32 * 1024 * 1024,
  chunkMaxBytes: 256 * 1024 * 1024,
  blocks: {
    early: { prepareUtc: '05:50', owns: '[API main start, 14:00 Europe/Moscow)' },
    late: { prepareUtc: '10:50', owns: '[14:00 Europe/Moscow, API main end)' },
  },
  splitMoscowTime: '14:00', launchLatenessMs: STUDY_LAUNCH_LATENESS_MS,
  jobMaxMinutes: STUDY_JOB_MAX_MS / 60_000,
  requiredDaysPerPhase: STUDY_REQUIRED_DAYS, maxAttemptedJobs: STUDY_MAX_ATTEMPTS,
  archive: { kind: 'private-draft-github-release', tag: STUDY_RELEASE_TAG, immutableAssets: true },
  quality: { minimumDailyTimerCoverage: 0.99, minimumDailyPerInstrumentCoverage: 0.8 },
  phases: ['DEVELOPMENT', 'READY_TO_FREEZE', 'HOLDOUT', 'COMPLETE'],
} as const;

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const object = value as Record<string, unknown>;
  return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`).join(',')}}`;
}
export function hashStudyValue(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}
export const STUDY_PROTOCOL_HASH = hashStudyValue(studyProtocol);

export interface StudyChunkPlan {
  index: number;
  plannedStart: string;
  plannedEnd: string;
}
export interface StudyBlockPlan {
  sessionDate: string;
  block: StudyBlock;
  mainStart: string;
  mainEnd: string;
  ownedStart: string;
  ownedEnd: string;
  prepareAt: string;
  captureNotBefore: string;
  latenessMs: number;
  chunks: StudyChunkPlan[];
  /** Collection recovery only; full-session scientific bounds remain unchanged. */
  recovery?: { requestedBlock: StudyBlock; selectedAt: string; partialStart: boolean };
}

function utcForMoscowDate(date: string, hour: number, minute: number): number {
  const parsed = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date);
  if (!parsed) throw new Error('Invalid Moscow study date');
  return Date.UTC(Number(parsed[1]), Number(parsed[2]) - 1, Number(parsed[3]), hour - 3, minute);
}

/** Manual preparation must be upcoming; delayed schedules defer admission to planStudyBlock. */
export function planStudyPreparation(block: StudyBlock, nowMs: number, source: 'manual' | 'schedule' = 'manual'): { sessionDate: string; readyAt: string } {
  if (!['early', 'late'].includes(block) || !Number.isFinite(nowMs)) throw new Error('Invalid preparation request');
  const sessionDate = new Date(nowMs + 3 * 3_600_000).toISOString().slice(0, 10);
  const target = utcForMoscowDate(sessionDate, block === 'early' ? 8 : 13, 50);
  const delay = target - nowMs;
  if ((delay <= 0 && source !== 'schedule') || delay >= STUDY_JOB_MAX_MS - 120_000) throw new Error('Preparation must be later today and within the bounded job duration');
  return { sessionDate, readyAt: new Date(target).toISOString() };
}

export function assertStudyPreparationReady(preparation: { sessionDate: string; readyAt: string }, nowMs: number): void {
  if (!Number.isFinite(nowMs) || new Date(nowMs + 3 * 3_600_000).toISOString().slice(0, 10) !== preparation.sessionDate
    || nowMs < Date.parse(preparation.readyAt)) throw new Error('Preparation wake-up crossed its date or occurred before its target');
}

export function planStudyBlock(
  sessionDate: string,
  mainStart: string,
  mainEnd: string,
  block: StudyBlock,
  nowMs: number,
): StudyBlockPlan | null {
  return buildStudyBlockPlan(sessionDate, mainStart, mainEnd, block, nowMs, false);
}

/** A late trigger collects the remaining real session without redefining its denominator. */
export function planRecoverableStudyBlock(
  sessionDate: string,
  mainStart: string,
  mainEnd: string,
  requestedBlock: StudyBlock,
  nowMs: number,
): StudyBlockPlan | null {
  if (!['early', 'late'].includes(requestedBlock) || !Number.isFinite(nowMs)) throw new Error('Invalid recovery request');
  if (new Date(nowMs + 3 * 3_600_000).toISOString().slice(0, 10) !== sessionDate) return null;
  const split = utcForMoscowDate(sessionDate, 14, 0);
  const block = requestedBlock === 'early' && nowMs >= split ? 'late' : requestedBlock;
  const plan = buildStudyBlockPlan(sessionDate, mainStart, mainEnd, block, nowMs, true);
  if (!plan) return null;
  if (block !== requestedBlock || plan.latenessMs > STUDY_LAUNCH_LATENESS_MS) {
    plan.recovery = { requestedBlock, selectedAt: new Date(nowMs).toISOString(), partialStart: nowMs > Date.parse(plan.ownedStart) };
  }
  return plan;
}

function buildStudyBlockPlan(
  sessionDate: string,
  mainStart: string,
  mainEnd: string,
  block: StudyBlock,
  nowMs: number,
  recoverLate: boolean,
): StudyBlockPlan | null {
  const start = Date.parse(mainStart), end = Date.parse(mainEnd);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start || !Number.isFinite(nowMs)) {
    throw new Error('Invalid study block clock');
  }
  const split = utcForMoscowDate(sessionDate, 14, 0);
  const prepare = utcForMoscowDate(sessionDate, block === 'early' ? 8 : 13, 50);
  const ownedStart = block === 'early' ? start : Math.max(start, split);
  const ownedEnd = block === 'early' ? Math.min(end, split) : end;
  if (!['early', 'late'].includes(block)) throw new Error('Invalid study block');
  if (ownedEnd <= ownedStart || (!recoverLate && nowMs < prepare) || nowMs >= ownedEnd
    || (!recoverLate && nowMs > ownedStart + STUDY_LAUNCH_LATENESS_MS)
    || (recoverLate && ownedEnd - Math.max(nowMs, ownedStart) < 120_000)) return null;
  const chunks: StudyChunkPlan[] = [];
  for (let cursor = ownedStart, index = 1; cursor < ownedEnd; cursor += STUDY_CHUNK_MAX_MS, index += 1) {
    chunks.push({ index, plannedStart: new Date(cursor).toISOString(), plannedEnd: new Date(Math.min(cursor + STUDY_CHUNK_MAX_MS, ownedEnd)).toISOString() });
  }
  if (ownedEnd - (recoverLate ? nowMs : Math.max(nowMs, prepare)) > STUDY_JOB_MAX_MS) return null;
  return {
    sessionDate, block, mainStart: new Date(start).toISOString(), mainEnd: new Date(end).toISOString(),
    ownedStart: new Date(ownedStart).toISOString(), ownedEnd: new Date(ownedEnd).toISOString(),
    prepareAt: new Date(prepare).toISOString(), captureNotBefore: new Date(ownedStart).toISOString(),
    latenessMs: Math.max(0, nowMs - ownedStart), chunks,
  };
}

export function chunkDurationSeconds(chunk: StudyChunkPlan, nowMs: number): number {
  const start = Math.max(Date.parse(chunk.plannedStart), nowMs);
  return Math.floor((Date.parse(chunk.plannedEnd) - start - STUDY_CHUNK_END_MARGIN_MS) / 1_000);
}
