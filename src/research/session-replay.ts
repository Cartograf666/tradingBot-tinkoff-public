import { readFileSync } from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { MarketDataResponse, type OrderBook } from 'tinkoff-invest-api/dist/generated/marketdata.js';
import { scanRecording, type RecordedEvent, type RecordingScanSummary } from './market-recording.js';
import { observationSubscriptions, subscriptionAcknowledgments, qualifyBook, classifyObservationSession, type ObservationManifest } from './market-observation.js';
import { assessBookCosts } from './order-book-costs.js';

export interface ReplayChunk {
  directory: string; manifest: ObservationManifest; manifestSha256: string;
  integrity: RecordingScanSummary; first: RecordedEvent; last: RecordedEvent;
}
const sha = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
export async function inspectReplayChunk(directory: string): Promise<ReplayChunk> {
  const bytes = readFileSync(path.join(directory, 'manifest.json'));
  const manifest = JSON.parse(bytes.toString('utf8')) as ObservationManifest;
  if (manifest.schemaVersion !== 1 || manifest.status !== 'COMPLETE' || !manifest.recording
    || manifest.source !== 'exchange' || !/sandbox/i.test(manifest.endpoint)
    || !manifest.instruments?.length || new Set(manifest.instruments.map(i => i.uid)).size !== manifest.instruments.length
    || manifest.instruments.some(i => !i.uid || !i.exchange || !Number.isSafeInteger(i.lot) || i.lot <= 0)
    || !Number.isFinite(manifest.settings?.durationMs) || manifest.settings.durationMs <= 0
    || manifest.settings.sampleIntervalMs !== 1_000 || manifest.settings.maxFutureSkewMs !== 1_000
    || manifest.settings.maxBookAgeMs !== 2_000 || !Array.isArray(manifest.intervals)) throw new Error('Replay requires a complete, valid exchange sandbox recording');
  if (manifest.settings.segmentMaxBytes !== undefined && !manifest.recording.segments) throw new Error('Missing segment manifest');
  let first: RecordedEvent | undefined, last: RecordedEvent | undefined;
  const integrity = await scanRecording(path.join(directory, 'events.ndjson'), event => {
    if (event.runId !== manifest.runId) throw new Error('Recording runId mismatch');
    const wall = Date.parse(event.receivedAt);
    if (!Number.isFinite(wall)) throw new Error('Invalid receipt time');
    first ??= event;
    // Compare cumulative wall/monotonic time as well as adjacent frames: small
    // repeated clock adjustments must not move a chunk into another session.
    for (const anchor of [first, last]) if (anchor) {
      const elapsed = Number(BigInt(event.monotonicOffsetNs) - BigInt(anchor.monotonicOffsetNs)) / 1e6;
      if (Math.abs(wall - Date.parse(anchor.receivedAt) - elapsed) > manifest.settings.maxFutureSkewMs) throw new Error('Local clock jump in recording');
    }
    last = event;
  }, manifest.recording.segments);
  if (!first || !last || !integrity.hasStop || integrity.truncatedTail
    || integrity.sha256 !== manifest.recording.sha256 || integrity.bytes !== manifest.recording.bytes
    || integrity.events !== manifest.recording.events) throw new Error('Recording integrity or completion mismatch');
  return { directory: path.resolve(directory), manifest, manifestSha256: sha(bytes), integrity, first, last };
}

function metadata(manifest: ObservationManifest) {
  return JSON.stringify({ source: manifest.source, endpoint: manifest.endpoint,
    instruments: manifest.instruments, depth: manifest.settings.depth, budget: manifest.settings.budgetRub,
    commission: manifest.settings.commissionRate, sourceHashes: manifest.codeHashes });
}

export function validateSessionChunks(chunks: ReplayChunk[], start: number, end: number): ReplayChunk[] {
  if (!chunks.length || !Number.isFinite(start) || !Number.isFinite(end) || end <= start) throw new Error('Invalid session bounds or empty session');
  const ordered = [...chunks].sort((a, b) => Date.parse(a.first.receivedAt) - Date.parse(b.first.receivedAt));
  const identity = metadata(ordered[0].manifest), runIds = new Set<string>();
  let previousEnd = start;
  for (const chunk of ordered) {
    const first = Date.parse(chunk.first.receivedAt), last = Date.parse(chunk.last.receivedAt);
    if (metadata(chunk.manifest) !== identity) throw new Error('Incompatible chunk metadata or recorder version');
    if (runIds.has(chunk.manifest.runId)) throw new Error('Duplicate recording in session');
    runIds.add(chunk.manifest.runId);
    if (first < start || last > end || first < previousEnd) throw new Error('Overlapping or out-of-session chunks');
    for (const instrument of chunk.manifest.instruments) {
      const interval = chunk.manifest.intervals.find(i => i.exchange.toUpperCase() === instrument.exchange.toUpperCase()
        && /^(regular|holiday)_trading_session_main$/.test(i.type) && Date.parse(i.start) <= start && Date.parse(i.end) >= end);
      if (!interval) throw new Error('Session bounds are not confirmed by each chunk API calendar');
    }
    previousEnd = last;
  }
  return ordered;
}

/** One daily receipt stream; broker payloads are unchanged. Original event IDs
 * are recoverable through the returned sequence ranges and immutable archives. */
export async function consumeSessionChunks(chunks: ReplayChunk[], manifest: ObservationManifest, start: number,
  consume: (event: RecordedEvent) => void) {
  let sequence = 0, nextEpoch = 0, previousEnd = start;
  const mappings: { runId: string; recordingSha256: string; dailySequenceStart: number; dailySequenceEnd: number }[] = [];
  consume({ schemaVersion: 1, runId: manifest.runId, sequence: ++sequence, connectionEpoch: 0,
    receivedAt: new Date(start).toISOString(), monotonicOffsetNs: '0', kind: 'gap', payload: { reason: 'DAY_START' } });
  let previousOffset = 0n;
  for (const chunk of chunks) {
    const firstAt = Date.parse(chunk.first.receivedAt), firstOffset = BigInt(chunk.first.monotonicOffsetNs);
    const base = BigInt(firstAt - start) * 1_000_000n;
    if (base < previousOffset) throw new Error('Monotonic chunk overlap');
    consume({ schemaVersion: 1, runId: manifest.runId, sequence: ++sequence, connectionEpoch: 0,
      receivedAt: chunk.first.receivedAt, monotonicOffsetNs: base.toString(), kind: 'gap',
      payload: { reason: 'CHUNK_BOUNDARY', gapMs: firstAt - previousEnd, originalRunId: chunk.manifest.runId } });
    const epochs = new Map<number, number>(), firstSequence = sequence + 1;
    const verified = await scanRecording(path.join(chunk.directory, 'events.ndjson'), event => {
      if (!epochs.has(event.connectionEpoch)) epochs.set(event.connectionEpoch, ++nextEpoch);
      const offset = base + BigInt(event.monotonicOffsetNs) - firstOffset;
      if (offset < previousOffset) throw new Error('Non-monotonic daily stream');
      consume({ ...event, runId: manifest.runId, sequence: ++sequence,
        connectionEpoch: epochs.get(event.connectionEpoch)!, monotonicOffsetNs: offset.toString() });
      previousOffset = offset;
    }, chunk.manifest.recording!.segments);
    if (verified.sha256 !== chunk.integrity.sha256) throw new Error('Recording changed during replay');
    mappings.push({ runId: chunk.manifest.runId, recordingSha256: verified.sha256,
      dailySequenceStart: firstSequence, dailySequenceEnd: sequence });
    previousEnd = Date.parse(chunk.last.receivedAt);
  }
  return mappings;
}

/** Timer coverage is a UNION of actual one-second bins against the entire
 * planned session. Reconnects, missing chunks and late starts never shrink it. */
export class SessionCoverage {
  private readonly recorded = new Set<number>();
  private readonly usable = new Map<string, Set<number>>();
  private readonly books = new Map<string, { book: OrderBook; offset: bigint }>();
  private readonly sourceTimes = new Map<string, number>();
  private readonly statuses = new Map<string, number>();
  private readonly acks = new Set<string>();
  private readonly expected: string[];
  private connected = false;
  private epoch = 0;
  constructor(private readonly manifest: ObservationManifest, private readonly start: number, private readonly end: number) {
    this.expected = observationSubscriptions(manifest.instruments);
    for (const i of manifest.instruments) this.usable.set(i.uid, new Set());
  }
  consume(event: RecordedEvent) {
    if (event.kind === 'connect_attempt' || ['gap', 'disconnect', 'stop', 'heartbeat_timeout', 'subscription_timeout'].includes(event.kind)) {
      this.connected = event.kind === 'connect_attempt'; this.epoch = event.connectionEpoch;
      this.books.clear(); this.sourceTimes.clear(); this.statuses.clear(); this.acks.clear(); return;
    }
    const wall = Date.parse(event.receivedAt), offset = BigInt(event.monotonicOffsetNs);
    if (event.kind === 'tick') {
      if (wall < this.start || wall >= this.end) return;
      const bin = Math.floor((wall - this.start) / 1_000); this.recorded.add(bin);
      if (!this.connected || event.connectionEpoch !== this.epoch || this.expected.some(k => !this.acks.has(k))) return;
      for (const instrument of this.manifest.instruments) {
        const saved = this.books.get(instrument.uid);
        if (!saved || Number(offset - saved.offset) / 1e6 > this.manifest.settings.maxBookAgeMs || this.statuses.get(instrument.uid) !== 5) continue;
        const quality = qualifyBook(saved.book, instrument, wall, this.manifest.settings);
        if (!quality.usable || quality.source !== 'EXCHANGE') continue;
        const session = classifyObservationSession(instrument, wall, quality.source, this.manifest.intervals);
        if (!session.regular || !session.sourceMatches || !/_main$/.test(session.phase)) continue;
        if (assessBookCosts(quality.book, { lotSize: instrument.lot, budgetRub: this.manifest.settings.budgetRub,
          commissionRate: this.manifest.settings.commissionRate }).status === 'AVAILABLE') this.usable.get(instrument.uid)!.add(bin);
      }
      return;
    }
    if (event.kind !== 'response' || !this.connected || event.connectionEpoch !== this.epoch) return;
    const frame = event.payload as { observationOrigin?: unknown; response?: { tradingStatuses?: { instrumentUid: string; tradingStatus: number }[] } };
    if (frame?.observationOrigin === 'UNARY_GET_TRADING_STATUSES') {
      for (const s of frame.response?.tradingStatuses ?? []) this.statuses.set(s.instrumentUid, s.tradingStatus);
      return;
    }
    for (const ack of subscriptionAcknowledgments(event.payload, this.manifest.source, this.manifest.settings.depth)) {
      if (ack.success) this.acks.add(ack.key); else { this.acks.delete(ack.key); this.books.clear(); }
    }
    const response = MarketDataResponse.fromJSON(event.payload);
    if (response.tradingStatus) this.statuses.set(response.tradingStatus.instrumentUid, response.tradingStatus.tradingStatus);
    if (response.orderbook) {
      const book = response.orderbook, instrument = this.manifest.instruments.find(i => i.uid === book.instrumentUid);
      const quality = qualifyBook(book, instrument, wall, this.manifest.settings);
      const previous = this.sourceTimes.get(book.instrumentUid);
      if (!quality.usable || quality.source !== 'EXCHANGE' || (previous !== undefined && quality.timestampMs < previous)) {
        this.books.delete(book.instrumentUid); return;
      }
      this.sourceTimes.set(book.instrumentUid, quality.timestampMs);
      this.books.set(book.instrumentUid, { book, offset });
    }
  }
  result() {
    const expectedTicks = Math.ceil((this.end - this.start) / 1_000);
    const recordedShare = this.recorded.size / expectedTicks;
    const perInstrument = this.manifest.instruments.map(i => ({ ticker: i.ticker,
      usableTicks: this.usable.get(i.uid)!.size, usableShare: this.usable.get(i.uid)!.size / expectedTicks }));
    return { status: recordedShare >= .99 && perInstrument.every(i => i.usableShare >= .8) ? 'PASS' as const : 'INSUFFICIENT_DATA' as const,
      recordedShare, perInstrument, expectedTicks, observedTicks: this.recorded.size };
  }
}
