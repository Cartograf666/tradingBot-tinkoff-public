import { createHash } from 'node:crypto';
import { existsSync, readFileSync, mkdirSync, writeFileSync, realpathSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defaultSimulationConfig, OrderBookSimulator, type SimulationResult } from '../research/order-book-simulator.js';
import { inspectReplayChunk, validateSessionChunks, consumeSessionChunks, SessionCoverage, type ReplayChunk } from '../research/session-replay.js';
import type { ObservationManifest } from '../research/market-observation.js';

const sha = (bytes: Buffer | string) => createHash('sha256').update(bytes).digest('hex');
const sourceFiles = ['src/report/replay-orderbook.ts', 'src/research/session-replay.ts',
  'src/research/continuous-windows.ts', 'src/research/continuous-checkpoints.ts',
  'src/research/order-book-simulator.ts', 'src/research/market-observation.ts',
  'src/research/market-recording.ts', 'src/research/order-book-costs.ts', 'package-lock.json'];
function projectRoot() {
  let directory = path.dirname(fileURLToPath(import.meta.url));
  while (!existsSync(path.join(directory, 'package.json'))) {
    const parent = path.dirname(directory); if (parent === directory) throw new Error('Cannot locate replay project'); directory = parent;
  }
  return directory;
}
export function replaySourceHashes(root = projectRoot()): Record<string, string> {
  return Object.fromEntries(sourceFiles.map(file => [file, sha(readFileSync(path.join(root, file)))]));
}
function replayRuntimeHashes() {
  const extension = path.extname(fileURLToPath(import.meta.url));
  const src = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  return Object.fromEntries(sourceFiles.filter(f => f.endsWith('.ts')).map(file => {
    const relative = file.replace(/^src\//, '').replace(/\.ts$/, extension);
    return [relative, sha(readFileSync(path.join(src, relative)))];
  }));
}
function resolveOutput(file: string): string {
  let ancestor = path.resolve(file);
  const missing: string[] = [];
  while (!existsSync(ancestor)) {
    missing.unshift(path.basename(ancestor)); ancestor = path.dirname(ancestor);
  }
  return path.join(realpathSync(ancestor), ...missing);
}
export function fixedReplayScenarios() {
  return (['momentum', 'exhaustion'] as const).flatMap(strategy => [
    { name: `${strategy}-baseline`, config: defaultSimulationConfig(strategy) },
    { name: `${strategy}-stress`, config: defaultSimulationConfig(strategy, { commissionRate: .001, latencyMs: 1_000 }) },
  ]);
}
export function replayConfigHash(): string { return sha(JSON.stringify(fixedReplayScenarios())); }
function summarize(name: string, result: SimulationResult) {
  const winning = result.closedTrades.reduce((sum, t) => sum + Math.max(0, t.pnlRub), 0);
  const losing = result.closedTrades.reduce((sum, t) => sum - Math.min(0, t.pnlRub), 0);
  return { name, strategy: result.config.strategy, scenario: name.endsWith('-stress') ? 'stress' : 'baseline',
    signals: result.signals, entries: result.fills.filter(f => f.side === 'BUY').length,
    closedTrades: result.closedTrades.length, netPnlRub: result.openPositions.length ? null : result.realizedPnlRub,
    realizedPnlRub: result.realizedPnlRub, feesRub: result.feesRub, profitFactor: losing ? winning / losing : null,
    maxDrawdownRub: result.maxDrawdownRub, unresolvedPositions: result.openPositions.length, economicSuccess: false as const };
}
export interface SessionReplayOptions { sessionDate: string; mainStart: string; mainEnd: string }

async function replay(chunks: ReplayChunk[], output: string, options: SessionReplayOptions, scope: 'recording' | 'session') {
  const start = Date.parse(options.mainStart), end = Date.parse(options.mainEnd);
  const ordered = validateSessionChunks(chunks, start, end);
  const datasetInputs = ordered.map(c => ({ runId: c.manifest.runId, manifestSha256: c.manifestSha256,
    recordingSha256: c.integrity.sha256, events: c.integrity.events, bytes: c.integrity.bytes,
    firstReceivedAt: c.first.receivedAt, lastReceivedAt: c.last.receivedAt, recorderHashes: c.manifest.codeHashes }));
  const datasetHash = sha(JSON.stringify({ scope, ...options, inputs: datasetInputs }));
  const manifest: ObservationManifest = { ...ordered[0].manifest, runId: `replay-${datasetHash}`, createdAt: options.mainStart,
    settings: { ...ordered[0].manifest.settings, durationMs: end - start }, recording: undefined,
    notes: ['Derived stream: original payloads preserved; global offsets start at the API session opening; every chunk boundary invalidates market state.'] };
  const scenarios = fixedReplayScenarios(), simulators = scenarios.map(s => new OrderBookSimulator(manifest, s.config));
  const coverage = new SessionCoverage(manifest, start, end);
  const mappings = await consumeSessionChunks(ordered, manifest, start, event => {
    coverage.consume(event); for (const simulator of simulators) simulator.consume(event);
  });
  const details = simulators.map(s => s.finish());
  const report = { schemaVersion: 1 as const,
    dataset: { scope, ...options, datasetHash, inputs: datasetInputs, mappings },
    configHash: replayConfigHash(), simulatorHashes: replaySourceHashes(), runtimeHashes: replayRuntimeHashes(),
    quality: coverage.result(), results: details.map((result, i) => summarize(scenarios[i].name, result)),
    limitations: ['Displayed-depth IOC simulation; queue priority and market impact are not observable.',
      'Drawdown measures realized equity at entry-cost inventory, not mark-to-market risk.',
      'Commission and latency are fixed scenarios, not verified account terms.',
      'Any open terminal inventory makes full net PnL unknown. A profitable replay does not establish an investable edge.'] };
  const destination = resolveOutput(output);
  for (const chunk of ordered) {
    const original = realpathSync(chunk.directory);
    if (destination === original || destination.startsWith(`${original}${path.sep}`)) throw new Error('Replay output must be separate from original recordings');
  }
  // Exclusive output prevents silent replacement of a previous experiment.
  mkdirSync(path.dirname(destination), { recursive: true }); mkdirSync(destination);
  const number = (n: number | null) => n === null ? 'не определён' : n.toFixed(2);
  const rows = report.results.map(r => `| ${r.name} | ${r.entries} | ${r.closedTrades} | ${number(r.netPnlRub)} | ${number(r.feesRub)} | ${r.unresolvedPositions} |`).join('\n');
  writeFileSync(path.join(destination, 'replay.json'), `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(path.join(destination, 'fills.json'), `${JSON.stringify(details, null, 2)}\n`);
  writeFileSync(path.join(destination, 'REPORT.md'), `# Симуляция по записанным стаканам\n\n${options.mainStart} — ${options.mainEnd}. Покрытие ${scope === 'session' ? 'всей основной сессии' : 'отдельной записи'}: ${(report.quality.recordedShare * 100).toFixed(2)}%; качество: ${report.quality.status}. Записей: ${ordered.length}.\n\n| Гипотеза и условия | Входы | Закрытия, включая частичные | Чистый результат, ₽ | Комиссии, ₽ | Незакрытые позиции |\n| --- | ---: | ---: | ---: | ---: | ---: |\n${rows}\n\nУ каждой гипотезы один капитал 100 000 ₽ на все акции и части дня, бюджет одного входа до 4 000 ₽. Покупки и продажи исполняются по первому допустимому полученному стакану после задержки: 300 мс и комиссия 0,05% на сторону в базовом сценарии; 1000 мс и 0,10% — в стрессовом. Часть заявки, которой не хватило видимой глубины, отменяется.\n\nЭто модель рыночных заявок по видимой глубине. Она не измеряет очередь и влияние своих сделок на рынок. Незакрытая позиция остаётся открытой, конечная цена не подставляется. Просадка отражает реализованный результат; риск открытых позиций ею не измеряется. Положительный результат одного дня ещё не подтверждает прибыльность стратегии.\n\n[Параметры, качество и происхождение](replay.json) · [Заявки и исполнения модели](fills.json)\n`);
  return report;
}
export type ReplayReport = Awaited<ReturnType<typeof replay>>;
export async function replayRecording(directory: string, output: string): Promise<ReplayReport> {
  const chunk = await inspectReplayChunk(directory);
  const start = Date.parse(chunk.first.receivedAt);
  // The declared capture duration is fixed before observation. Never move the
  // exit deadline forward based on how late the final event happened to arrive.
  const end = start + chunk.manifest.settings.durationMs;
  const terminalAt = Date.parse(chunk.last.receivedAt);
  if (terminalAt > end + 2_000) throw new Error('Capture exceeds declared duration');
  // Recorder stop can follow its final timer by milliseconds. Diagnostic bounds
  // include that stop; full-day bounds always come from the authoritative API.
  return replay([chunk], output, { sessionDate: new Date(start + 3 * 3600_000).toISOString().slice(0, 10),
    mainStart: chunk.first.receivedAt, mainEnd: new Date(Math.max(end, terminalAt)).toISOString() }, 'recording');
}
export async function replaySession(directories: string[], output: string, options: SessionReplayOptions): Promise<ReplayReport> {
  if (new Date(Date.parse(options.mainStart) + 3 * 3600_000).toISOString().slice(0, 10) !== options.sessionDate) throw new Error('Session date mismatch');
  const chunks: ReplayChunk[] = [];
  for (const directory of directories) chunks.push(await inspectReplayChunk(directory));
  return replay(chunks, output, options, 'session');
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args.length !== 2) { console.error('Usage: replay-orderbook <recording-directory> <new-output-directory>'); process.exitCode = 1; }
  else replayRecording(args[0], args[1]).then(r => console.log(JSON.stringify({ quality: r.quality, results: r.results }))).catch(error => { console.error(error instanceof Error ? error.message : 'Replay failed'); process.exitCode = 1; });
}
