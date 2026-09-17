import path from 'node:path';
import { scanRecording } from './market-recording.js';
import { inspectReplayChunk, SessionCoverage, type ReplayChunk } from './session-replay.js';
import type { StudyChunkPlan } from './study-protocol.js';

/** A view of an immutable continuous recording.  It deliberately contains no
 * event range: callers retain the raw run as the sole evidence and use these
 * planned bounds only as a coverage denominator. */
export interface ContinuousWindowPlan extends Pick<StudyChunkPlan, 'index' | 'plannedStart' | 'plannedEnd'> {}

export interface ContinuousWindowChecks {
  completeRecording: boolean;
  captureCompletedByDuration: boolean;
  exchangeSource: boolean;
  mainSessionConfirmed: boolean;
  timerCoverage: boolean;
  perInstrumentCoverage: boolean;
}

export interface ContinuousWindowCoverage {
  expectedTicks: number;
  observedTicks: number;
  recordedShare: number;
  perInstrument: Array<{ ticker: string; usableTicks: number; usableShare: number }>;
}

export interface ContinuousWindowAssessment {
  index: number;
  plannedStart: string;
  plannedEnd: string;
  quality: 'PASS' | 'INSUFFICIENT_DATA';
  checks: ContinuousWindowChecks;
  coverage: ContinuousWindowCoverage;
}

interface ValidatedWindow extends ContinuousWindowPlan { start: number; end: number }

function validateWindows(chunks: readonly ContinuousWindowPlan[]): ValidatedWindow[] {
  if (!chunks.length) throw new Error('Continuous window assessment requires planned chunks');
  const indexes = new Set<number>();
  const windows = chunks.map(chunk => {
    const start = Date.parse(chunk.plannedStart), end = Date.parse(chunk.plannedEnd);
    if (!Number.isSafeInteger(chunk.index) || chunk.index <= 0 || indexes.has(chunk.index)
      || !Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
      throw new Error('Invalid continuous window plan');
    }
    indexes.add(chunk.index);
    return { ...chunk, plannedStart: new Date(start).toISOString(), plannedEnd: new Date(end).toISOString(), start, end };
  }).sort((a, b) => a.start - b.start || a.end - b.end || a.index - b.index);
  for (let index = 1; index < windows.length; index += 1) {
    if (windows[index].start < windows[index - 1].end) throw new Error('Continuous window plans overlap');
  }
  return windows;
}

function mainSessionConfirmed(chunk: ReplayChunk, start: number, end: number): boolean {
  return chunk.manifest.instruments.every(instrument => chunk.manifest.intervals.some(interval =>
    interval.exchange.toUpperCase() === instrument.exchange.toUpperCase()
      && /^(regular|holiday)_trading_session_main$/.test(interval.type)
      && Date.parse(interval.start) <= start && Date.parse(interval.end) >= end,
  ));
}

/**
 * Assess planned 30-minute windows from one continuous raw run.  The stream is
 * read exactly once in receipt order.  Each window sees earlier authentic
 * connection/status/book events for warm-up, while SessionCoverage itself only
 * admits timer ticks whose receipt time is inside that window.
 */
export async function assessContinuousWindows(
  directory: string,
  chunks: readonly ContinuousWindowPlan[],
): Promise<ContinuousWindowAssessment[]> {
  const windows = validateWindows(chunks);
  const replay = await inspectReplayChunk(directory);
  const coverages = windows.map(window => new SessionCoverage(replay.manifest, window.start, window.end));

  // A second scanner pass is intentional: `inspectReplayChunk` establishes the
  // immutable COMPLETE snapshot; this pass both feeds it and proves it did not
  // change before analysis completed.
  const scanned = await scanRecording(path.join(replay.directory, 'events.ndjson'), event => {
    for (const coverage of coverages) coverage.consume(event);
  }, replay.manifest.recording!.segments);
  if (scanned.sha256 !== replay.integrity.sha256 || scanned.bytes !== replay.integrity.bytes
    || scanned.events !== replay.integrity.events || scanned.truncatedTail || !scanned.hasStop) {
    throw new Error('Continuous recording changed or lost integrity during assessment');
  }

  return windows.map((window, index) => {
    const result = coverages[index].result();
    const checks: ContinuousWindowChecks = {
      completeRecording: replay.manifest.status === 'COMPLETE',
      // COMPLETE is also used for user/deadline aborts by the recorder.  A
      // scientific window may pass only when the stream itself reached its
      // duration terminal condition; small tolerated timer gaps are assessed
      // below against the fixed planned denominator.
      captureCompletedByDuration: replay.manifest.capture?.reason === 'duration',
      exchangeSource: replay.manifest.source === 'exchange',
      mainSessionConfirmed: mainSessionConfirmed(replay, window.start, window.end),
      timerCoverage: result.recordedShare >= .99,
      perInstrumentCoverage: result.perInstrument.every(instrument => instrument.usableShare >= .8),
    };
    const passes = Object.values(checks).every(Boolean);
    return {
      index: window.index, plannedStart: window.plannedStart, plannedEnd: window.plannedEnd,
      quality: passes && result.status === 'PASS' ? 'PASS' : 'INSUFFICIENT_DATA',
      checks,
      coverage: {
        expectedTicks: result.expectedTicks, observedTicks: result.observedTicks, recordedShare: result.recordedShare,
        perInstrument: result.perInstrument,
      },
    };
  });
}
