import type { ReplayReport } from './replay-orderbook.js';
import { hashStudyValue, planStudyBlock, type StudyBlockPlan, type StudyChunkPlan } from '../research/study-protocol.js';
import type { StudyChunkReceipt, StudyLedger } from '../research/study-state.js';

/** A closed-session replay is a diagnostic artifact, never an acceptance record. */
export interface DailyResearchIdentityInput {
  replayConfigHash: string;
  simulatorHashes: Record<string, string>;
  fixedScenarios: unknown;
  /** Hashes only report-rendering sources; it must not alter replay/acquisition identity. */
  reportVersionHash: string;
}

export type DailyResearchInputSelection = {
  action: 'open' | 'noinput';
  sessionDate: string;
  phase: 'DEVELOPMENT' | 'HOLDOUT' | null;
  plan: StudyBlockPlan;
  receipts: StudyChunkReceipt[];
  fullDayAccepted: boolean;
} | {
  action: 'closed';
  sessionDate: string;
  phase: 'DEVELOPMENT' | 'HOLDOUT';
  plan: StudyBlockPlan;
  receipts: StudyChunkReceipt[];
  fullDayAccepted: boolean;
};

function receiptKey(receipt: StudyChunkReceipt): string {
  return `${receipt.sessionDate}:${receipt.block}:${receipt.chunkIndex}`;
}

function expectedDayChunks(plan: StudyBlockPlan): Array<{ key: string; chunk: StudyChunkPlan }> {
  const early = planStudyBlock(plan.sessionDate, plan.mainStart, plan.mainEnd, 'early', Date.parse(`${plan.sessionDate}T05:50:00.000Z`));
  const late = planStudyBlock(plan.sessionDate, plan.mainStart, plan.mainEnd, 'late', Date.parse(`${plan.sessionDate}T10:50:00.000Z`));
  if (!early || !late) throw new Error('Cannot reconstruct full-day research bounds');
  return [early, late].flatMap(block => block.chunks.map(chunk => ({ key: `${plan.sessionDate}:${block.block}:${chunk.index}`, chunk })));
}

function matchesExpectedReceipt(receipt: StudyChunkReceipt, key: string, chunk: StudyChunkPlan): boolean {
  return receipt.chunkId === key && receiptKey(receipt) === key && receipt.plannedStart === chunk.plannedStart
    && receipt.plannedEnd === chunk.plannedEnd;
}

function acceptedDay(ledger: StudyLedger, plan: StudyBlockPlan, phase: 'DEVELOPMENT' | 'HOLDOUT' | null): boolean {
  return phase !== null && ledger.days.some(day => day.sessionDate === plan.sessionDate && day.phase === phase
    && day.mainStart === plan.mainStart && day.mainEnd === plan.mainEnd && day.quality.status === 'PASS');
}

/**
 * Preserve canonical choices; where the scientific gate accepted no archive,
 * use the first durable receipt regardless of quality. Corrupt mappings fail
 * instead of producing a smaller, deceptively plausible replay.
 */
export function selectDailyResearchInputs(ledger: StudyLedger, plan: StudyBlockPlan, nowMs = Date.now()): DailyResearchInputSelection {
  if (!Number.isFinite(nowMs) || !Number.isFinite(Date.parse(plan.mainEnd))) throw new Error('Invalid daily research clock');
  const expected = expectedDayChunks(plan), expectedByKey = new Map(expected.map(item => [item.key, item.chunk]));
  const assets = new Map<number, string>();
  for (const receipt of ledger.chunks.filter(item => item.sessionDate === plan.sessionDate)) {
    const previous = assets.get(receipt.assetId);
    if (previous) throw new Error(`Asset ${receipt.assetId} appears more than once in daily chunk receipts`);
    assets.set(receipt.assetId, receipt.chunkId);
    const chunk = expectedByKey.get(receipt.chunkId);
    if (!chunk || !matchesExpectedReceipt(receipt, receipt.chunkId, chunk)) throw new Error(`Chunk ${receipt.chunkId} does not match full-day planned bounds`);
  }
  const receipts: StudyChunkReceipt[] = [];
  for (const { key, chunk } of expected) {
    const canonicalAssetId = ledger.canonicalChunks[key];
    const candidates = ledger.chunks.filter(receipt => matchesExpectedReceipt(receipt, key, chunk));
    let selected: StudyChunkReceipt | undefined;
    if (canonicalAssetId !== undefined) {
      const canonical = ledger.chunks.filter(receipt => receipt.assetId === canonicalAssetId);
      if (canonical.length !== 1 || !matchesExpectedReceipt(canonical[0]!, key, chunk)) throw new Error(`Canonical chunk ${key} has no unique matching receipt`);
      selected = canonical[0]!;
    } else if (candidates.length) {
      selected = [...candidates].sort((left, right) => left.uploadedAt.localeCompare(right.uploadedAt) || left.assetId - right.assetId)[0]!;
    }
    if (selected) receipts.push(selected);
  }
  receipts.sort((left, right) => left.plannedStart.localeCompare(right.plannedStart) || left.chunkIndex - right.chunkIndex);
  const phases = new Set(receipts.map(receipt => receipt.phase));
  if (phases.size > 1) throw new Error('Daily research inputs span multiple phases');
  const phase = phases.values().next().value as 'DEVELOPMENT' | 'HOLDOUT' | undefined;
  const fullDayAccepted = acceptedDay(ledger, plan, phase ?? null);
  if (nowMs < Date.parse(plan.mainEnd)) return { action: 'open', sessionDate: plan.sessionDate,
    phase: phase ?? null, plan, receipts, fullDayAccepted };
  if (!receipts.length || !phase) return { action: 'noinput', sessionDate: plan.sessionDate,
    phase: phase ?? null, plan, receipts, fullDayAccepted };
  if (phase !== 'DEVELOPMENT') throw new Error('Automatic daily diagnostics are limited to DEVELOPMENT');
  return { action: 'closed', sessionDate: plan.sessionDate, phase, plan, receipts, fullDayAccepted };
}

export function dailyResearchIdentity(selection: Extract<DailyResearchInputSelection, { action: 'closed' }>, input: DailyResearchIdentityInput): string {
  return hashStudyValue({ schemaVersion: 1, kind: 'daily-diagnostic-research', sessionDate: selection.sessionDate,
    phase: selection.phase, fullDayAccepted: selection.fullDayAccepted, bounds: { mainStart: selection.plan.mainStart, mainEnd: selection.plan.mainEnd },
    inputs: selection.receipts.map(receipt => ({ chunkId: receipt.chunkId, assetId: receipt.assetId,
      archiveSha256: receipt.archiveSha256, assetDigest: receipt.assetDigest, manifestHash: receipt.manifestHash,
      recordingHash: receipt.recordingHash })), replayConfigHash: input.replayConfigHash,
    simulatorHashes: input.simulatorHashes, fixedScenarios: input.fixedScenarios, reportVersionHash: input.reportVersionHash });
}

export interface DailyDiagnosticResearchReport {
  schemaVersion: 1;
  kind: 'daily-diagnostic-research';
  identity: string;
  diagnosticOnly: true;
  counted: false;
  formalHoldout: false;
  sessionDate: string;
  fullDayAccepted: boolean;
  session: { date: string; phase: 'DEVELOPMENT' | 'HOLDOUT'; mainStart: string; mainEnd: string };
  inputs: Array<{ assetId: number; chunkId: string; assetDigest: string; archiveSha256: string; manifestHash: string; recordingHash: string; plannedStart: string; plannedEnd: string; quality: StudyChunkReceipt['quality'] }>;
  replay: Pick<ReplayReport, 'configHash' | 'simulatorHashes' | 'runtimeHashes' | 'dataset' | 'quality' | 'limitations'> & { fixedScenarios: unknown; results: ReplayReport['results'] };
  reportVersionHash: string;
}

export function buildDailyResearchReport(
  selection: Extract<DailyResearchInputSelection, { action: 'closed' }>,
  replay: ReplayReport,
  input: DailyResearchIdentityInput,
): { json: DailyDiagnosticResearchReport; markdown: string } {
  if (replay.configHash !== input.replayConfigHash || hashStudyValue(replay.simulatorHashes) !== hashStudyValue(input.simulatorHashes)) {
    throw new Error('Replay implementation identity differs from the requested report identity');
  }
  const names = Array.isArray(input.fixedScenarios) ? input.fixedScenarios.map(item =>
    typeof item === 'object' && item !== null && 'name' in item && typeof item.name === 'string' ? item.name : null) : [];
  if (replay.dataset.scope !== 'session' || replay.dataset.sessionDate !== selection.sessionDate
    || replay.dataset.mainStart !== selection.plan.mainStart || replay.dataset.mainEnd !== selection.plan.mainEnd
    || names.length !== 4 || names.some(name => name === null) || new Set(names).size !== 4
    || replay.results.length !== 4 || replay.results.some((result, index) => result.name !== names[index])) {
    throw new Error('Daily diagnostic report requires all four fixed scenarios');
  }
  const identity = dailyResearchIdentity(selection, input);
  const json: DailyDiagnosticResearchReport = { schemaVersion: 1, kind: 'daily-diagnostic-research', identity,
    diagnosticOnly: true, counted: false, formalHoldout: false, sessionDate: selection.sessionDate, fullDayAccepted: selection.fullDayAccepted,
    session: { date: selection.sessionDate, phase: selection.phase, mainStart: selection.plan.mainStart, mainEnd: selection.plan.mainEnd },
    inputs: selection.receipts.map(receipt => ({ assetId: receipt.assetId, chunkId: receipt.chunkId,
      assetDigest: receipt.assetDigest, archiveSha256: receipt.archiveSha256, manifestHash: receipt.manifestHash,
      recordingHash: receipt.recordingHash, plannedStart: receipt.plannedStart, plannedEnd: receipt.plannedEnd, quality: receipt.quality })),
    replay: { configHash: replay.configHash, simulatorHashes: replay.simulatorHashes, runtimeHashes: replay.runtimeHashes,
      dataset: replay.dataset, quality: replay.quality, limitations: replay.limitations, fixedScenarios: input.fixedScenarios,
      results: replay.results }, reportVersionHash: input.reportVersionHash };
  return { json, markdown: renderDailyResearchReport(json) };
}

export function renderDailyResearchReport(report: DailyDiagnosticResearchReport): string {
  const number = (value: number | null) => value === null ? 'не определён' : value.toFixed(2);
  const rows = report.replay.results.map(result => `| ${result.name} | ${result.entries} | ${result.closedTrades} | ${number(result.netPnlRub)} | ${number(result.realizedPnlRub)} | ${number(result.feesRub)} | ${number(result.maxDrawdownRub)} | ${result.unresolvedPositions} |`).join('\n');
  return `# Ежедневный диагностический отчёт\n\n`
    + `Идентификатор: \`${report.identity}\`. Диагностика: **да**; учитывается в исследовании: **нет**; формальный holdout: **нет**. `
    + `Полный день принят ledger: **${report.fullDayAccepted ? 'да' : 'нет'}**.\n\n`
    + `Полный знаменатель сессии сохранён: ${report.session.mainStart} — ${report.session.mainEnd}. `
    + `Использовано архивов для диагностики: ${report.inputs.length}; фаза: ${report.session.phase}.\n\n`
    + `| Фиксированный сценарий | Входы | Закрытые сделки | Чистый PnL, ₽ | Реализованный PnL, ₽ | Комиссии, ₽ | Реализованная просадка, ₽ | Незакрытые позиции |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n${rows}\n\n`
    + `Чистый PnL остаётся «не определён», если есть незакрытая позиция; нули не подставляются. Просадка здесь **реализованная**, не mark-to-market.\n\n`
    + `Качество replay: ${report.replay.quality.status}, покрытие ${(report.replay.quality.recordedShare * 100).toFixed(2)}%. `
    + `Сырые input asset ID: ${report.inputs.map(receipt => receipt.assetId).join(', ')}.\n\n`
    + `Конфигурация четырёх фиксированных сценариев: \`${JSON.stringify(report.replay.fixedScenarios)}\`. `
    + `Хеш конфигурации: \`${report.replay.configHash}\`; хеши симулятора: \`${JSON.stringify(report.replay.simulatorHashes)}\`; runtime-хеши: \`${JSON.stringify(report.replay.runtimeHashes)}\`; версия отчёта: \`${report.reportVersionHash}\`.\n\n`
    + `Ограничения исходного replay:\n${report.replay.limitations.map(item => `- ${item}`).join('\n')}\n`;
}
