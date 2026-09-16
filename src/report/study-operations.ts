import type { StudyBlockPlan } from '../research/study-protocol.js';
import type { StudyLedger } from '../research/study-state.js';

/** Operational evidence is separate from immutable scientific acceptance. */
export interface BlockOperation {
  schemaVersion: 1;
  attemptId: string;
  plan: StudyBlockPlan;
  startedAt: string;
  updatedAt: string;
  state: 'STARTING' | 'CAPTURING' | 'FINISHED' | 'FAILED' | 'CANCELLED';
  failure: 'STARTUP' | 'RECORDING' | 'STORAGE_OR_PROCESSING' | null;
  parts: Array<{ index: number; status: 'SAVED' | 'RECORDING_FAILED'; assetId: number;
    quality: 'PASS' | 'INSUFFICIENT_DATA' | null; reasons: string[] }>;
}

export function operationalDay(ledger: StudyLedger, operations: BlockOperation[], date: string, now = Date.now()) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(Date.parse(`${date}T00:00:00Z`)) || !Number.isFinite(now)) {
    throw new Error('Invalid operational report date');
  }
  const chunks = ledger.chunks.filter(item => item.sessionDate === date);
  const attempts = ledger.attempts.filter(item => item.sessionDate === date);
  const blocks = operations.filter(item => item.plan.sessionDate === date);
  const accepted = ledger.days.find(item => item.sessionDate === date);
  const usable = new Set(chunks.filter(item => item.quality === 'PASS'
    && ledger.canonicalChunks[item.chunkId] === item.assetId).map(item => item.chunkId));
  const rejected = new Set(chunks.filter(item => item.quality !== 'PASS').map(item => item.chunkId));
  const lastUpload = chunks.map(item => item.uploadedAt).sort().at(-1) ?? null;
  const end = blocks[0]?.plan.mainEnd;
  const closed = end ? now >= Date.parse(end) : date < new Date(now + 3 * 3_600_000).toISOString().slice(0, 10);
  const state = accepted?.quality.status === 'PASS' ? 'DAY_ACCEPTED'
    : accepted || closed ? 'INCOMPLETE_DAY'
    : blocks.some(item => item.state === 'FAILED' || item.state === 'CANCELLED') ? 'INTERRUPTED'
    : chunks.length ? 'PARTIAL_DATA' : 'NO_DATA';
  return { schemaVersion: 1 as const, sessionDate: date, state, generatedAt: new Date(now).toISOString(),
    fullDayAccepted: accepted?.quality.status === 'PASS', attempts: attempts.length,
    confirmedArchives: chunks.length, passingParts: usable.size, rejectedParts: rejected.size,
    failedRecordings: blocks.reduce((count, item) => count + item.parts.filter(part => part.status === 'RECORDING_FAILED').length, 0),
    lastConfirmedUpload: lastUpload, mainStart: blocks[0]?.plan.mainStart ?? accepted?.mainStart ?? null,
    mainEnd: end ?? accepted?.mainEnd ?? null,
    dailyQuality: accepted?.quality ?? null,
    blocks: blocks.map(item => ({ block: item.plan.block, state: item.state, failure: item.failure,
      plannedParts: item.plan.chunks.length, savedParts: item.parts.filter(part => part.status === 'SAVED').length,
      updatedAt: item.updatedAt })),
    meaning: 'Archive and quality status only. A running workflow or a saved part does not establish full-day completeness or profitability.' };
}

export function renderOperationalDay(day: ReturnType<typeof operationalDay>): string {
  const labels: Record<string, string> = { DAY_ACCEPTED: 'Полный день прошёл качество', INCOMPLETE_DAY: 'Неполный день — не засчитан',
    INTERRUPTED: 'Сбор прерван', PARTIAL_DATA: 'Сохранена часть данных', NO_DATA: 'Данные не получены' };
  return `### ${day.sessionDate}: ${labels[day.state]}\n\n`
    + `Подтверждено архивов: **${day.confirmedArchives}**. Частей с пройденным качеством: **${day.passingParts}**; отклонённых: **${day.rejectedParts}**.\n\n`
    + `Неудачных записей с отдельной диагностикой: ${day.failedRecordings}. Последняя подтверждённая загрузка: ${day.lastConfirmedUpload ?? 'нет'}.\n\n`
    + (day.fullDayAccepted ? 'День включён в исследование; это не подтверждение прибыльности.\n'
      : 'Полный день пока не подтверждён. Эти данные не засчитываются как успешный день исследования.\n');
}

export function closedPlansNeedingFinalization(ledger: StudyLedger, operations: BlockOperation[], now = Date.now()): StudyBlockPlan[] {
  const plans = new Map<string, StudyBlockPlan>();
  for (const operation of operations) {
    const plan = operation.plan;
    if (Date.parse(plan.mainEnd) <= now && !ledger.days.some(day => day.sessionDate === plan.sessionDate)) {
      plans.set(plan.sessionDate, plan);
    }
  }
  return [...plans.values()];
}

/** Continue only a classified temporary metadata failure; everything else requires a visible stop. */
export function recoverableRecordingFailure(manifest: Record<string, unknown>, deadlineMs?: number, now = Date.now()): boolean {
  const failure = manifest.failure as Record<string, unknown> | undefined;
  const slotExpired = failure?.category === 'DEADLINE' && deadlineMs !== undefined
    && Number.isFinite(deadlineMs) && now >= deadlineMs;
  return manifest.status === 'FAILED' && failure?.stage === 'metadata' && (slotExpired || (failure.retryable === true
    && ['TIMEOUT', 'UNAVAILABLE', 'RESOURCE_EXHAUSTED'].includes(String(failure.category))));
}

/** An exhausted temporary metadata request can be retried within the same owned slot, never indefinitely. */
export async function recordOwnedSlot<T>(options: {
  deadlineMs: number; signal: AbortSignal; now?: () => number;
  attempt: (attempt: number) => Promise<{ value: T; retryable: boolean }>;
  wait: (delayMs: number, signal: AbortSignal) => Promise<void>;
}): Promise<T> {
  const now = options.now ?? Date.now;
  if (!Number.isFinite(options.deadlineMs)) throw new Error('Invalid slot deadline');
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    options.signal.throwIfAborted();
    if (now() >= options.deadlineMs) throw new Error('Slot deadline expired');
    const result = await options.attempt(attempt);
    options.signal.throwIfAborted();
    if (!result.retryable || attempt === 2 || now() + 20_000 >= options.deadlineMs) return result.value;
    await options.wait(5_000, options.signal);
  }
  throw new Error('Unreachable slot attempt');
}
