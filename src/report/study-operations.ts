import { planStudyBlock, type StudyBlockPlan } from '../research/study-protocol.js';
import type { StudyLedger } from '../research/study-state.js';
import type { StudyRuntimeSnapshot } from './study-runtime.js';

/** Operational evidence is separate from immutable scientific acceptance. */
export interface BlockOperation {
  schemaVersion: 1;
  attemptId: string;
  plan: StudyBlockPlan;
  startedAt: string;
  updatedAt: string;
  state: 'STARTING' | 'CAPTURING' | 'FINISHED' | 'FAILED' | 'CANCELLED';
  failure: 'STARTUP' | 'RECORDING' | 'STORAGE_OR_PROCESSING' | null;
  currentChunkIndex?: number;
  parts: Array<{ index: number; status: 'SAVED' | 'RECORDING_FAILED'; assetId: number;
    quality: 'PASS' | 'INSUFFICIENT_DATA' | null; reasons: string[] }>;
}

/** Runtime evidence is fresh API data; a stored CAPTURING flag alone never proves liveness. */
export function collectorStatus(operations: BlockOperation[], date: string, now: number, runtime?: StudyRuntimeSnapshot) {
  const base = { checkedAt: runtime?.checkedAt ?? null, nextExpectedUploadAt: null as string | null,
    uploadOverdueSeconds: 0, runIds: [] as string[], reason: null as string | null };
  if (date !== new Date(now + 3 * 3_600_000).toISOString().slice(0, 10)) return { ...base, status: 'HISTORICAL' };
  if (!runtime?.available) return { ...base, status: 'UNKNOWN', reason: runtime?.reason ?? 'NOT_CHECKED' };
  base.runIds = runtime.runs.map(run => run.runId);
  const current = operations.filter(op => op.plan.sessionDate === date).sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  const capture = runtime.runs.find(run => run.captureCommandRunning && ['arm', 'campaign'].includes(run.mode));
  if (capture) {
    const operation = current.find(op => op.attemptId.split(':')[0] === capture.runId);
    if (operation?.state === 'CAPTURING') {
      const expected = operation.plan.chunks.find(chunk => operation.currentChunkIndex !== undefined
        ? chunk.index === operation.currentChunkIndex && !operation.parts.some(part => part.index === chunk.index && part.status === 'SAVED')
        : Date.parse(chunk.plannedEnd) > Date.parse(operation.startedAt)
          && !operation.parts.some(part => part.index === chunk.index));
      if (expected) {
        base.nextExpectedUploadAt = new Date(Date.parse(expected.plannedEnd) + 180_000).toISOString();
        base.uploadOverdueSeconds = Math.max(0, Math.floor((now - Date.parse(base.nextExpectedUploadAt)) / 1000));
      }
      if (!expected) return { ...base, status: 'PROCESSING' };
      return { ...base, status: base.uploadOverdueSeconds ? 'UPLOAD_OVERDUE' : 'CAPTURING' };
    }
    return { ...base, status: 'STARTING' };
  }
  if (runtime.runs.some(run => run.captureCommandRunning)) return { ...base, status: 'DIAGNOSTIC' };
  if (runtime.runs.some(run => run.captureJobRunning)) return { ...base, status: 'STARTING' };
  if (runtime.runs.some(run => run.preparing || run.queued)) return { ...base, status: 'WAITING' };
  return { ...base, status: current[0] && ['STARTING', 'CAPTURING', 'FAILED', 'CANCELLED'].includes(current[0].state) ? 'STOPPED' : 'IDLE' };
}

function unarchivedWindows(ledger: StudyLedger, plan: StudyBlockPlan | undefined, date: string, now: number) {
  if (!plan) return { count: null, seconds: null };
  const expected = (['early', 'late'] as const).flatMap(block => {
    const at = Date.parse(`${date}T${block === 'early' ? '08' : '13'}:50:00+03:00`);
    return (planStudyBlock(date, plan.mainStart, plan.mainEnd, block, at)?.chunks ?? []).map(chunk => ({ ...chunk, block }));
  });
  const missing = expected.filter(chunk => Date.parse(chunk.plannedEnd) <= now && !ledger.chunks.some(receipt =>
    receipt.sessionDate === date && receipt.block === chunk.block && receipt.chunkIndex === chunk.index
    && receipt.plannedStart === chunk.plannedStart && receipt.plannedEnd === chunk.plannedEnd));
  return { count: missing.length, seconds: missing.reduce((total, chunk) => total
    + (Date.parse(chunk.plannedEnd) - Date.parse(chunk.plannedStart)) / 1000, 0) };
}

export function operationalDay(ledger: StudyLedger, operations: BlockOperation[], date: string, now = Date.now(), runtime?: StudyRuntimeSnapshot) {
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
  const missing = unarchivedWindows(ledger, blocks[0]?.plan, date, now);
  return { schemaVersion: 1 as const, sessionDate: date, state, generatedAt: new Date(now).toISOString(),
    collector: collectorStatus(blocks, date, now, runtime),
    unarchivedClosedWindows: missing.count, unarchivedClosedWindowSeconds: missing.seconds,
    fullDayAccepted: accepted?.quality.status === 'PASS', attempts: attempts.length,
    confirmedArchives: chunks.length, passingParts: usable.size, rejectedParts: rejected.size,
    failedRecordings: blocks.reduce((count, item) => count + item.parts.filter(part => part.status === 'RECORDING_FAILED').length, 0),
    lastConfirmedUpload: lastUpload, mainStart: blocks[0]?.plan.mainStart ?? accepted?.mainStart ?? null,
    mainEnd: end ?? accepted?.mainEnd ?? null,
    dailyQuality: accepted?.quality ?? null,
    blocks: blocks.map(item => ({ block: item.plan.block, state: item.state, failure: item.failure,
      recovery: item.plan.recovery ?? null,
      plannedParts: item.plan.chunks.length, savedParts: item.parts.filter(part => part.status === 'SAVED').length,
      updatedAt: item.updatedAt })),
    meaning: 'Archive and quality status only. A running workflow or a saved part does not establish full-day completeness or profitability.' };
}

export function renderOperationalDay(day: ReturnType<typeof operationalDay>): string {
  const labels: Record<string, string> = { DAY_ACCEPTED: 'Полный день прошёл качество', INCOMPLETE_DAY: 'Неполный день — не засчитан',
    INTERRUPTED: 'Сбор прерван', PARTIAL_DATA: 'Сохранена часть данных', NO_DATA: 'Данные не получены' };
  const runtimeLabels: Record<string, string> = { HISTORICAL: 'Завершённая дата', UNKNOWN: 'Активность не подтверждена',
    CAPTURING: 'Идёт запись', UPLOAD_OVERDUE: 'Задача работает, но следующий архив задерживается', STARTING: 'Запуск / проверка подключения',
    PROCESSING: 'Обработка сохранённых результатов',
    DIAGNOSTIC: 'Выполняется диагностическая запись', WAITING: 'Подготовка / ожидание очереди', STOPPED: 'Сбор остановлен', IDLE: 'Активного сбора нет' };
  return `### ${day.sessionDate}: ${labels[day.state]}\n\n`
    + `Сборщик: **${runtimeLabels[day.collector.status]}**. Проверка активности: ${day.collector.checkedAt ?? 'не выполнена'}.\n\n`
    + (day.collector.nextExpectedUploadAt ? `Следующий архив ожидается не позднее ${day.collector.nextExpectedUploadAt} (граница части + 3 минуты на сохранение).\n\n` : '')
    + `Подтверждено архивов: **${day.confirmedArchives}**. Частей с пройденным качеством: **${day.passingParts}**; отклонённых: **${day.rejectedParts}**.\n\n`
    + `Неудачных записей с отдельной диагностикой: ${day.failedRecordings}. Последняя подтверждённая загрузка: ${day.lastConfirmedUpload ?? 'нет'}.\n\n`
    + (day.unarchivedClosedWindows === null ? 'Нет подтверждённого календаря для подсчёта пропущенных интервалов.\n\n'
      : `Завершившихся интервалов без подтверждённого архива: **${day.unarchivedClosedWindows}** (${Math.round(day.unarchivedClosedWindowSeconds! / 60)} минут). Это интервалы без архива, а не оценка свежести данных внутри сохранённых частей.\n\n`)
    + 'Диагностические smoke/observe-архивы учитываются отдельно и не входят в эти счётчики исследования.\n\n'
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
