import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { MarketDataResponse, type OrderBook } from 'tinkoff-invest-api/dist/generated/marketdata.js';
import { assessBookCosts, type AvailableBookCosts } from './order-book-costs.js';
import { scanRecording, scanUnfinalizedRecording, type RecordedEvent } from './market-recording.js';
import {
  bookSource, tradeSource, classifyObservationSession, qualifyBook, qualifyTrade,
  subscriptionAcknowledgments, observationSubscriptions, type ObservationManifest, type ObservationSource,
} from './market-observation.js';

interface Group {
  ticker: string; source: ObservationSource;
  bookEvents: number; tradeEvents: number; validTradeEvents: number;
  regularTradeEvents: number; tradeLots: number;
  rejectedBooks: Record<string, number>; rejectedTrades: Record<string, number>;
  sampleExclusions: Record<string, number>; phases: Record<string, number>;
  sampled: number; regularSamples: number;
  spreadBps: number[]; costsRub: number[]; breakEvenBps: number[];
  stressBreakEvenBps: number[]; oneLotBreakEvenBps: number[];
  targetNetRiseBps: number[]; ageMs: number[]; lastCost: AvailableBookCosts | null;
}
const increment = (r: Record<string, number>, key: string) => { r[key] = (r[key] ?? 0) + 1; };
const costArrayKeys = ['spreadBps', 'costsRub', 'breakEvenBps', 'stressBreakEvenBps', 'oneLotBreakEvenBps', 'targetNetRiseBps', 'ageMs'] as const;
type PhaseCosts = Pick<Group, typeof costArrayKeys[number] | 'sampled' | 'lastCost'> & {
  observedScheduledTicks: number; exclusions: Record<string, number>;
};
function emptyPhaseCosts(): PhaseCosts {
  return { sampled: 0, lastCost: null, observedScheduledTicks: 0, exclusions: {}, spreadBps: [], costsRub: [], breakEvenBps: [], stressBreakEvenBps: [], oneLotBreakEvenBps: [], targetNetRiseBps: [], ageMs: [] };
}

/** Bind both reviewed sources and the actually loaded TS/JS modules when rebuilding an archive. */
export function observationAnalysisProvenance() {
  const directory = path.dirname(fileURLToPath(import.meta.url));
  let root = directory;
  while (!existsSync(path.join(root, 'package.json'))) {
    const parent = path.dirname(root); if (parent === root) throw new Error('Cannot locate analysis project'); root = parent;
  }
  const modules = ['market-recording-report', 'market-observation', 'market-recording', 'order-book-costs'];
  const hash = (p: string) => createHash('sha256').update(readFileSync(p)).digest('hex');
  const extension = path.extname(fileURLToPath(import.meta.url));
  return {
    sourceHashes: { ...Object.fromEntries(modules.map(name => [`src/research/${name}.ts`, hash(path.join(root, `src/research/${name}.ts`))])), 'package-lock.json': hash(path.join(root, 'package-lock.json')) },
    runtimeHashes: Object.fromEntries(modules.map(name => [`${name}${extension}`, hash(path.join(directory, `${name}${extension}`))])),
  };
}
function distribution(values: number[]) {
  if (!values.length) return { samples: 0, mean: null, median: null, p95: null, min: null, max: null };
  const sorted = [...values].sort((a, b) => a - b), middle = Math.floor(sorted.length / 2);
  return { samples: values.length, mean: values.reduce((a, b) => a + b, 0) / values.length,
    median: sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2,
    p95: sorted[Math.ceil(sorted.length * .95) - 1], min: sorted[0], max: sorted.at(-1)! };
}

/** Consumes in local receipt order, never sorts by exchange time or deduplicates trades. */
export class ObservationAccumulator {
  private readonly groups = new Map<string, Group>();
  private readonly phaseCosts = new Map<string, Map<string, PhaseCosts>>();
  private readonly latestBooks = new Map<string, { book: OrderBook; epoch: number; offsetNs: bigint; receivedAt: number }>();
  private readonly lastSourceTimes = new Map<string, number>();
  private readonly acks = new Set<string>();
  private readonly successfulAcks = new Set<string>();
  private readonly statuses = new Map<string, number>();
  private currentEpoch = 0;
  private connected = false;
  private firstEvent: RecordedEvent | null = null;
  private lastEvent: RecordedEvent | null = null;
  private lastSampleOffset: bigint | null = null;
  private totalTicks = 0;
  private responses = 0;
  private statusSnapshots = 0;
  private bookEvents = 0;
  private tradeEvents = 0;
  private sourceReorders = 0;
  private clockJumps = 0;
  private disconnects = 0;
  private gapMarkers = 0;
  private readonly ackFailures: Record<string, number> = {};

  constructor(private readonly manifest: ObservationManifest) {
    for (const instrument of manifest.instruments) for (const source of ['EXCHANGE', 'DEALER', 'UNKNOWN'] as const) {
      this.groups.set(`${instrument.uid}:${source}`, {
        ticker: instrument.ticker, source, bookEvents: 0, tradeEvents: 0, validTradeEvents: 0,
        regularTradeEvents: 0, tradeLots: 0, rejectedBooks: {}, rejectedTrades: {}, sampleExclusions: {}, phases: {},
        sampled: 0, regularSamples: 0, spreadBps: [], costsRub: [], breakEvenBps: [], stressBreakEvenBps: [],
        oneLotBreakEvenBps: [], targetNetRiseBps: [], ageMs: [], lastCost: null,
      });
    }
  }

  consume(event: RecordedEvent): void {
    if (event.runId !== this.manifest.runId) throw new Error('Recording runId does not match manifest');
    const offset = BigInt(event.monotonicOffsetNs), wall = Date.parse(event.receivedAt);
    if (this.lastEvent) {
      const elapsed = Number(offset - BigInt(this.lastEvent.monotonicOffsetNs)) / 1e6;
      if (Math.abs(wall - Date.parse(this.lastEvent.receivedAt) - elapsed) > this.manifest.settings.maxFutureSkewMs) {
        this.latestBooks.clear(); this.clockJumps++;
      }
    }
    this.firstEvent ??= event;
    this.lastEvent = event;
    if (event.kind === 'connect_attempt') {
      this.currentEpoch = event.connectionEpoch; this.connected = true;
      this.latestBooks.clear(); this.acks.clear(); this.statuses.clear(); this.lastSourceTimes.clear();
      return;
    }
    if (['disconnect', 'gap', 'heartbeat_timeout', 'subscription_timeout', 'stop'].includes(event.kind)) {
      this.latestBooks.clear(); this.acks.clear(); this.statuses.clear(); this.connected = false;
      if (event.kind === 'disconnect') this.disconnects++;
      if (event.kind === 'gap') this.gapMarkers++;
      return;
    }
    if (event.kind === 'tick') {
      // A late timer is one observation, not permission to fabricate missed seconds.
      if (this.lastSampleOffset === null || Number(offset - this.lastSampleOffset) / 1e6 >= this.manifest.settings.sampleIntervalMs * .9) {
        this.sample(wall, offset); this.lastSampleOffset = offset; this.totalTicks++;
      }
      return;
    }
    if (event.kind !== 'response') return;
    const frame = event.payload as { observationOrigin?: unknown; response?: { tradingStatuses?: { instrumentUid?: unknown; tradingStatus?: unknown }[] } } | null;
    if (frame?.observationOrigin === 'UNARY_GET_TRADING_STATUSES') {
      if (event.connectionEpoch !== this.currentEpoch || !this.connected) return;
      this.statusSnapshots++;
      for (const status of frame.response?.tradingStatuses ?? []) {
        if (typeof status.instrumentUid === 'string' && this.manifest.instruments.some(i => i.uid === status.instrumentUid)
          && typeof status.tradingStatus === 'number' && Number.isInteger(status.tradingStatus)) this.statuses.set(status.instrumentUid, status.tradingStatus);
      }
      return;
    }
    this.responses++;
    const r = MarketDataResponse.fromJSON(event.payload);
    for (const ack of subscriptionAcknowledgments(event.payload, this.manifest.source, this.manifest.settings.depth)) {
      if (ack.success) { this.acks.add(ack.key); this.successfulAcks.add(ack.key); }
      else { this.acks.delete(ack.key); increment(this.ackFailures, ack.key); this.latestBooks.clear(); }
    }
    if (r.tradingStatus) this.statuses.set(r.tradingStatus.instrumentUid, r.tradingStatus.tradingStatus);
    if (r.orderbook) {
      this.bookEvents++;
      const book = r.orderbook, source = bookSource(book), key = `${book.instrumentUid}:${source}`;
      const group = this.groups.get(key), instrument = this.manifest.instruments.find(i => i.uid === book.instrumentUid);
      if (!group) return;
      group.bookEvents++;
      const quality = qualifyBook(book, instrument, wall, this.manifest.settings);
      if (!quality.usable) { increment(group.rejectedBooks, quality.reason); this.latestBooks.delete(key); return; }
      const previousTime = this.lastSourceTimes.get(`book:${key}`);
      if (previousTime !== undefined && quality.timestampMs < previousTime) {
        this.sourceReorders++; increment(group.rejectedBooks, 'OUT_OF_ORDER'); this.latestBooks.delete(key); return;
      }
      this.lastSourceTimes.set(`book:${key}`, quality.timestampMs);
      this.latestBooks.set(key, { book, epoch: event.connectionEpoch, offsetNs: offset, receivedAt: wall });
    }
    if (r.trade) {
      this.tradeEvents++;
      const trade = r.trade, source = tradeSource(trade), key = `${trade.instrumentUid}:${source}`;
      const group = this.groups.get(key), instrument = this.manifest.instruments.find(i => i.uid === trade.instrumentUid);
      if (!group || !instrument) return;
      group.tradeEvents++;
      const invalid = qualifyTrade(trade, instrument, wall, this.manifest.settings);
      if (invalid) { increment(group.rejectedTrades, invalid); return; }
      const time = trade.time!.getTime(), previousTime = this.lastSourceTimes.get(`trade:${key}`);
      // Count every identical trade. Without a trade ID it is unsafe to remove it.
      if (previousTime !== undefined && time < previousTime) { this.sourceReorders++; increment(group.rejectedTrades, 'OUT_OF_ORDER'); return; }
      this.lastSourceTimes.set(`trade:${key}`, time);
      group.validTradeEvents++; group.tradeLots += trade.quantity;
      const session = classifyObservationSession(instrument, time, source, this.manifest.intervals);
      if (session.regular && session.sourceMatches && this.acks.has(`trade:${instrument.uid}`)
        && this.connected && event.connectionEpoch === this.currentEpoch && this.statuses.get(instrument.uid) === (source === 'EXCHANGE' ? 5 : 14)) group.regularTradeEvents++;
    }
  }

  private sample(wall: number, offset: bigint): void {
    for (const [key, group] of this.groups) {
      if (group.source === 'UNKNOWN' || (this.manifest.source !== 'all' && group.source.toLowerCase() !== this.manifest.source)) continue;
      const instrument = this.manifest.instruments.find(i => `${i.uid}:${group.source}` === key)!;
      const session = classifyObservationSession(instrument, wall, group.source, this.manifest.intervals);
      let costs: PhaseCosts | undefined;
      if (session.regular && session.sourceMatches) {
        const phases = this.phaseCosts.get(key) ?? new Map<string, PhaseCosts>(); this.phaseCosts.set(key, phases);
        costs = phases.get(session.phase) ?? emptyPhaseCosts(); phases.set(session.phase, costs);
        costs.observedScheduledTicks++;
      }
      const exclude = (reason: string) => { increment(group.sampleExclusions, reason); if (costs) increment(costs.exclusions, reason); };
      const saved = this.latestBooks.get(key);
      let exclusion: string | null = !this.connected ? 'DISCONNECTED' : !this.acks.has(`book:${instrument.uid}`) ? 'NO_BOOK_ACK' : !saved ? 'NO_VALID_BOOK' : null;
      if (saved && (saved.epoch !== this.currentEpoch || Number(offset - saved.offsetNs) / 1e6 > this.manifest.settings.maxBookAgeMs)) exclusion = 'STALE_OR_PREVIOUS_CONNECTION';
      if (exclusion || !saved) { exclude(exclusion ?? 'NO_VALID_BOOK'); continue; }
      const quality = qualifyBook(saved.book, instrument, wall, this.manifest.settings);
      if (!quality.usable) { exclude(quality.reason); continue; }
      const assessment = assessBookCosts(quality.book, {
        lotSize: instrument.lot, budgetRub: this.manifest.settings.budgetRub, commissionRate: this.manifest.settings.commissionRate,
      });
      if (assessment.status !== 'AVAILABLE') { exclude(assessment.reason); continue; }
      increment(group.phases, session.phase);
      const status = this.statuses.get(instrument.uid);
      const statusMatches = status === (group.source === 'EXCHANGE' ? 5 : 14);
      if (!costs || !statusMatches) {
        exclude(status === undefined ? 'UNKNOWN_TRADING_STATUS' : !statusMatches ? 'TRADING_STATUS_NOT_REGULAR_FOR_SOURCE' : 'OUTSIDE_CONFIRMED_SOURCE_SESSION'); continue;
      }
      group.sampled++; group.regularSamples++;
      costs.sampled++;
      costs.spreadBps.push(assessment.spreadBps); costs.costsRub.push(assessment.roundTripLossRub);
      costs.breakEvenBps.push(assessment.breakEvenRiseBps); costs.ageMs.push(quality.ageMs);
      // +0.10% net on entry cash (including its fee), from current executable bid VWAP.
      costs.targetNetRiseBps.push(((assessment.requiredExitPrice * 1.001 / assessment.sellVwap) - 1) * 10000);
      costs.lastCost = assessment;
      const stress = assessBookCosts(quality.book, { lotSize: instrument.lot, budgetRub: this.manifest.settings.budgetRub, commissionRate: this.manifest.settings.commissionRate * 2 });
      if (stress.status === 'AVAILABLE') costs.stressBreakEvenBps.push(stress.breakEvenRiseBps);
      const oneLot = assessBookCosts(quality.book, { lotSize: instrument.lot, budgetRub: quality.book.asks[0].price * instrument.lot * (1 + this.manifest.settings.commissionRate), commissionRate: this.manifest.settings.commissionRate });
      if (oneLot.status === 'AVAILABLE') costs.oneLotBreakEvenBps.push(oneLot.breakEvenRiseBps);
    }
  }

  result() {
    const groups = [...this.groups.entries()].filter(([, g]) => g.source !== 'UNKNOWN' || g.bookEvents || g.tradeEvents).map(([key, g]) => ({
      ticker: g.ticker, source: g.source, bookEvents: g.bookEvents, tradeEvents: g.tradeEvents,
      validTradeEvents: g.validTradeEvents, regularTradeEvents: g.regularTradeEvents, observedTradeLots: g.tradeLots,
      rejectedBooks: g.rejectedBooks, rejectedTrades: g.rejectedTrades, sampleExclusions: g.sampleExclusions, phaseCounts: g.phases,
      eligibleSamples: g.sampled, eligibleSecondsApprox: g.sampled * this.manifest.settings.sampleIntervalMs / 1000,
      phases: [...(this.phaseCosts.get(key) ?? new Map<string, PhaseCosts>())].map(([phase, c]) => ({
        observedScheduledTicks: c.observedScheduledTicks, usableShareOfObservedScheduledTicks: c.observedScheduledTicks ? c.sampled / c.observedScheduledTicks : null,
        sampleExclusions: c.exclusions,
        phase, eligibleSamples: c.sampled, spreadBps: distribution(c.spreadBps), roundTripLossRub: distribution(c.costsRub), breakEvenRiseBps: distribution(c.breakEvenBps),
        doubleCommissionBreakEvenBps: distribution(c.stressBreakEvenBps), oneLotBreakEvenBps: distribution(c.oneLotBreakEvenBps),
        netTargetPointOnePctRiseBps: distribution(c.targetNetRiseBps), bookAgeMs: distribution(c.ageMs), lastAssessment: c.lastCost,
      })),
    }));
    const exchangeSamples = groups.filter(g => g.source === 'EXCHANGE').reduce((s, g) => s + g.eligibleSamples, 0);
    const dealerSamples = groups.filter(g => g.source === 'DEALER').reduce((s, g) => s + g.eligibleSamples, 0);
    const elapsedSeconds = this.firstEvent && this.lastEvent ? Number(BigInt(this.lastEvent.monotonicOffsetNs) - BigInt(this.firstEvent.monotonicOffsetNs)) / 1e9 : 0;
    const expectedTicks = Math.floor(elapsedSeconds * 1000 / this.manifest.settings.sampleIntervalMs);
    return {
      runId: this.manifest.runId, firstReceivedAt: this.firstEvent?.receivedAt ?? null, lastReceivedAt: this.lastEvent?.receivedAt ?? null,
      elapsedSeconds,
      samplingCoverage: { expectedTicksFromElapsedTime: expectedTicks, recordedTicks: this.totalTicks,
        missingTicks: Math.max(0, expectedTicks - this.totalTicks),
        recordedShare: expectedTicks ? Math.min(1, this.totalTicks / expectedTicks) : null },
      responses: this.responses, statusSnapshotResponses: this.statusSnapshots, bookEvents: this.bookEvents, tradeEvents: this.tradeEvents, samplingTicks: this.totalTicks,
      successfulSubscriptions: [...this.successfulAcks].sort(), expectedSubscriptions: observationSubscriptions(this.manifest.instruments),
      allSubscriptionsAcknowledged: observationSubscriptions(this.manifest.instruments).every(key => this.successfulAcks.has(key)),
      ackFailures: this.ackFailures, disconnects: this.disconnects, gapMarkers: this.gapMarkers, sourceReorders: this.sourceReorders, clockJumps: this.clockJumps,
      exchangeSamples, dealerSamples,
      evidence: exchangeSamples ? 'EXCHANGE_OBSERVATIONS_RECEIVED' : dealerSamples ? 'DEALER_OBSERVATIONS_ONLY' : 'NO_USABLE_MARKET_SAMPLE',
      groups,
    };
  }
}

export async function reportMarketRecording(directory: string) {
  const manifestBytes = readFileSync(path.join(directory, 'manifest.json'));
  const manifest = JSON.parse(manifestBytes.toString('utf8')) as ObservationManifest;
  if (manifest.schemaVersion !== 1 || !Array.isArray(manifest.instruments) || !Array.isArray(manifest.intervals)
    || !Number.isFinite(manifest.settings?.maxBookAgeMs) || manifest.settings.maxBookAgeMs <= 0
    || !Number.isFinite(manifest.settings?.commissionRate) || manifest.settings.commissionRate < 0 || manifest.settings.commissionRate >= .5
    || !Number.isFinite(manifest.settings?.sampleIntervalMs) || manifest.settings.sampleIntervalMs < 100) throw new Error('Invalid observation manifest');
  const analysisProvenance = observationAnalysisProvenance();
  for (const [file, hash] of Object.entries(analysisProvenance.sourceHashes)) {
    if (manifest.codeHashes?.[file] !== hash) throw new Error(`Analysis source differs from capture manifest: ${file}. Preserve the archive; use its recorded version for a faithful rebuild.`);
  }
  const accumulator = new ObservationAccumulator(manifest);
  const recoveredUnfinalized = manifest.settings.segmentMaxBytes !== undefined && !manifest.recording;
  if (manifest.settings.segmentMaxBytes !== undefined && manifest.recording && !manifest.recording.segments) throw new Error('Finalized segmented recording is missing its segment manifest');
  const recordingPath = path.join(directory, 'events.ndjson');
  const consume = (event: RecordedEvent) => accumulator.consume(event);
  const integrity = recoveredUnfinalized ? await scanUnfinalizedRecording(recordingPath, consume)
    : await scanRecording(recordingPath, consume, manifest.recording?.segments);
  if (manifest.recording && (integrity.sha256 !== manifest.recording.sha256 || integrity.bytes !== manifest.recording.bytes || integrity.events !== manifest.recording.events)) throw new Error('Recording checksum or length differs from completed manifest');
  const complete = !recoveredUnfinalized && integrity.hasStop && !integrity.truncatedTail && manifest.status === 'COMPLETE';
  const observed = accumulator.result();
  const capacity = { bytes: integrity.bytes, segmentCount: integrity.segments?.length ?? 1,
    bytesPerSecond: observed.elapsedSeconds > 0 ? integrity.bytes / observed.elapsedSeconds : null,
    totalCapBytes: manifest.settings.maxBytes, totalCapUsedShare: integrity.bytes / manifest.settings.maxBytes };
  const result = { ...observed, complete, integrity, capacity,
    recovery: { recoveredUnfinalized, comparedWithFinalManifest: Boolean(manifest.recording) },
    analysisProvenance, manifestSha256: createHash('sha256').update(manifestBytes).digest('hex'),
    model: { budgetRub: manifest.settings.budgetRub, commissionRatePerSide: manifest.settings.commissionRate,
      extraSlippageBpsPerSide: 0, meaning: 'Hypothetical buy and sell against the same displayed snapshot. No actual order, future-return estimate or fill guarantee.',
      sampling: 'One observation per recorded timer tick, normally 1 s. No filling missed ticks or stale books. No trade deduplication.' } };
  writeFileSync(path.join(directory, 'summary.json'), `${JSON.stringify(result, null, 2)}\n`);
  const number = (value: number | null, digits = 2) => value === null ? '—' : value.toLocaleString('ru-RU', { maximumFractionDigits: digits, minimumFractionDigits: digits });
  const percent = (bps: number | null) => bps === null ? '—' : `${number(bps / 100)}%`;
  const sourceLabel = (source: ObservationSource) => source === 'EXCHANGE' ? 'Биржа' : 'Дилер';
  const phaseLabel = (phase: string) => phase.includes('holiday') ? 'выходной' : phase.endsWith('_morning') ? 'утро' : phase.endsWith('_evening') ? 'вечер' : phase.endsWith('_main') ? 'основная' : 'прочая';
  const rows = result.groups.filter(g => g.source !== 'UNKNOWN').flatMap(g => g.phases.length ? g.phases.map(p => `| ${g.ticker} | ${sourceLabel(g.source)}, ${phaseLabel(p.phase)} | ${p.eligibleSamples}/${p.observedScheduledTicks} | ${percent(p.spreadBps.median)} | ${number(p.roundTripLossRub.median)} | ${percent(p.breakEvenRiseBps.median)} | ${percent(p.netTargetPointOnePctRiseBps.median)} |`)
    : [`| ${g.ticker} | ${sourceLabel(g.source)}: нет пригодной фазы | 0 | — | — | — | — |`]).join('\n');
  const title = result.exchangeSamples ? 'Биржевые наблюдения получены. Доходность стратегии не измерялась.' : result.dealerSamples ? 'Получены только пригодные дилерские наблюдения. Биржевая выборка ещё не собрана.' : 'Пригодных рыночных наблюдений для оценки стоимости пока нет.';
  const report = `# Запись рынка и стоимость сделки\n\n**${title}**\n\nПериод: ${result.firstReceivedAt ?? '—'} — ${result.lastReceivedAt ?? '—'} (UTC), ${number(result.elapsedSeconds)} с. Запись ${complete ? 'штатно завершена' : 'частичная или завершилась с ошибкой'}. Серверных ответов: ${result.responses}; подтверждено подписок ${result.successfulSubscriptions.length}/${result.expectedSubscriptions.length}. Стаканов: ${result.bookEvents}; обезличенных сделок: ${result.tradeEvents}.\n\nБюджет покупки до ${number(manifest.settings.budgetRub)} ₽ с округлением до лота. Комиссия ${number(manifest.settings.commissionRate * 100)}% на каждую сторону — сценарий sandbox, не подтверждённый реальный тариф владельца. Дополнительное проскальзывание в базовой оценке равно нулю: проход по видимой глубине уже учтён.\n\n| Акция | Источник и сессия | Пригодных / наблюдавшихся срезов | Медианный спред | Стоимость цикла, ₽ | Рост до безубыточности | Рост для +0,10% чистыми |\n| --- | --- | ---: | ---: | ---: | ---: | ---: |\n${rows}\n\n«Стоимость цикла» — гипотетические покупка по предложениям продавцов и продажа по предложениям покупателей в одном снимке, включая обе комиссии. Это расход, а не полученный убыток счёта. Порог роста отсчитывается от доступной сейчас средней цены продажи. Фактическое исполнение, изменение глубины за время задержки и очередь лимитных заявок этим расчётом не гарантируются.\n\nПригодные срезы требуют свежего консистентного стакана, правильного инструмента, подтверждённой подписки и известного подходящего торгового статуса текущего соединения, достаточной глубины и подходящего интервала из актуального расписания API. Биржевые и дилерские данные, а также утренняя, основная и вечерняя фазы никогда не объединяются в одну медиану. Распределения считаются на периодических срезах; частое обновление одной книги не даёт ей больший вес. Недостающие секунды не достраиваются.\n\nРазрывов соединения: ${result.disconnects}; маркеров неопределённости: ${result.gapMarkers}; перестановок времени источника: ${result.sourceReorders}; скачков локальных часов: ${result.clockJumps}. После разрыва прежний стакан исключается до новых подписок и данных. Одинаковые сделки сохраняются: в ответе SDK нет уникального идентификатора сделки. Метки времени источника имеют миллисекундную точность SDK; локальный порядок отдельно сохранён монотонным счётчиком.\n\nСнимков торгового статуса по отдельному запросу: ${result.statusSnapshotResponses}; они сохранены в том же журнале и входят в контрольную сумму. Повторная генерация отчёта проверяет исходники анализатора и lockfile; смена версии требует отдельного опыта. Причина остановки: ${manifest.capture?.reason ?? manifest.failure?.stage ?? 'неизвестна'}.\n\nОбъём записи: ${number(capacity.bytes / 1024 / 1024)} МиБ в ${capacity.segmentCount} файлах; средний темп ${number(capacity.bytesPerSecond === null ? null : capacity.bytesPerSecond / 1024)} КиБ/с. Записано тиков ${result.samplingCoverage.recordedTicks}, ожидаемо по длительности ${result.samplingCoverage.expectedTicksFromElapsedTime}, пропущено ${result.samplingCoverage.missingTicks}. Доля пригодных срезов в таблице рассчитана по реально наблюдавшимся тикам соответствующей фазы; пропущенное время оценивается отдельно и не выдаётся за полное покрытие.\n\nВосстановление незавершённой записи: ${recoveredUnfinalized ? 'ДА: последовательность восстановлена по файлам, финального манифеста для сверки нет' : 'нет'}.\n\nКонтрольная сумма последовательности файлов: ${integrity.sha256}. Оборванный хвост: ${integrity.truncatedTail ? 'ДА' : 'нет'}. JSON-сводка содержит причины исключения, медиану/среднее/p95, возраст книг, расчёт одного лота и двойной комиссии. Это проверка инструмента наблюдения; многодневная статистика сигнала и прибыльность ещё не проверены.\n\n[Сводка JSON](${path.join(directory, 'summary.json')}) · [Параметры и происхождение](${path.join(directory, 'manifest.json')})\n`;
  writeFileSync(path.join(directory, 'REPORT.md'), report);
  return result;
}
