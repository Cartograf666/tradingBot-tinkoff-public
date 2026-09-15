import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import { performance } from 'node:perf_hooks';
import test from 'node:test';
import { captureMarketStream } from './market-recorder.js';
import type { RecordedEventKind } from './market-recording.js';

interface CapturedEvent {
  kind: RecordedEventKind;
  payload: unknown;
  epoch: number;
}

function quietStream(): AsyncIterable<unknown> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<unknown> {
      return {
        next: () => new Promise<IteratorResult<unknown>>(() => undefined),
        return: () => new Promise<IteratorResult<unknown>>(() => undefined),
      };
    },
  };
}

function eventsRecorder(events: CapturedEvent[]) {
  return (kind: RecordedEventKind, payload: unknown, epoch: number): void => {
    events.push({ kind, payload, epoch });
  };
}

test('fractional monotonic start preserves the final tick at the exact capture boundary', async (t) => {
  const startedAt = 200.001;
  let now = startedAt, frames = 0;
  t.mock.method(performance, 'now', () => now);
  const events: CapturedEvent[] = [];
  const result = await captureMarketStream({
    openStream: () => ({ [Symbol.asyncIterator]: () => ({ next: async () => {
      now = startedAt + (++frames) * 1000;
      return { done: false, value: {} };
    } }) }),
    record: eventsRecorder(events), expectedSubscriptions: [], acknowledgments: () => [],
    durationMs: 60_000, tickIntervalMs: 1000, heartbeatTimeoutMs: 5000, maxReconnects: 0,
  });
  assert.equal(result.reason, 'duration');
  assert.equal(result.ticks, 60);
  assert.deepEqual(events.filter(event => event.kind === 'tick').map(event => event.payload),
    Array.from({ length: 60 }, (_, index) => ({ elapsedMs: (index + 1) * 1000, skippedIntervals: 0 })));
  assert.equal(events.at(-1)?.kind, 'stop');
});

test('subscription timeout aborts its epoch and reconnects until acknowledgments complete', async () => {
  const events: CapturedEvent[] = [];
  let opens = 0;
  const result = await captureMarketStream({
    openStream: () => {
      opens += 1;
      if (opens === 1) return quietStream();
      return {
        async *[Symbol.asyncIterator]() {
          yield { acknowledgments: [{ key: 'orderbook', success: true }] };
          await new Promise(() => undefined);
        },
      };
    },
    record: eventsRecorder(events),
    expectedSubscriptions: ['orderbook'],
    acknowledgments: (response) => (response as { acknowledgments?: Array<{ key: string; success: boolean }> }).acknowledgments ?? [],
    durationMs: 55,
    subscriptionTimeoutMs: 12,
    heartbeatTimeoutMs: 100,
    tickIntervalMs: 5,
    maxReconnects: 1,
    backoffMs: [1],
  });

  assert.equal(result.reason, 'duration');
  assert.equal(result.epochs, 2);
  assert.equal(result.acknowledgments, 1);
  assert.equal(events.filter((event) => event.kind === 'subscription_timeout').length, 1);
  assert.equal(events.some((event) => event.kind === 'gap'), true);
  assert.equal(events.at(-1)?.kind, 'stop');
});

test('failed acknowledgment is recorded raw before a bounded reconnect', async () => {
  const events: CapturedEvent[] = [];
  let opens = 0;
  const result = await captureMarketStream({
    openStream: () => ({
      async *[Symbol.asyncIterator]() {
        opens += 1;
        yield { acknowledgments: [{ key: 'trades', success: opens > 1 }] };
        await new Promise(() => undefined);
      },
    }),
    record: eventsRecorder(events),
    expectedSubscriptions: ['trades'],
    acknowledgments: (response) => (response as { acknowledgments: Array<{ key: string; success: boolean }> }).acknowledgments,
    durationMs: 35,
    subscriptionTimeoutMs: 10,
    heartbeatTimeoutMs: 100,
    tickIntervalMs: 5,
    maxReconnects: 1,
    backoffMs: [0],
  });
  assert.equal(result.reason, 'duration');
  assert.equal(result.epochs, 2);
  assert.equal(result.responses, 2);
  assert.equal(events.find((event) => event.kind === 'gap')?.payload &&
    (events.find((event) => event.kind === 'gap')!.payload as { reason: string }).reason, 'subscription_failed');
});

test('heartbeat uses any response and times out a subscribed but quiet stream', async () => {
  const events: CapturedEvent[] = [];
  const result = await captureMarketStream({
    openStream: () => ({
      async *[Symbol.asyncIterator]() {
        yield { ack: true };
        await new Promise(() => undefined);
      },
    }),
    record: eventsRecorder(events),
    expectedSubscriptions: ['info'],
    acknowledgments: (response) => (response as { ack?: boolean }).ack ? [{ key: 'info', success: true }] : [],
    durationMs: 100,
    subscriptionTimeoutMs: 20,
    heartbeatTimeoutMs: 12,
    tickIntervalMs: 5,
    maxReconnects: 0,
  });
  assert.equal(result.reason, 'max_reconnects');
  assert.equal(events.filter((event) => event.kind === 'heartbeat_timeout').length, 1);
  assert.equal(events.at(-1)?.kind, 'stop');
});

test('external stop during backoff cancels reconnect and sanitizes stream errors', async () => {
  const events: CapturedEvent[] = [];
  const controller = new AbortController();
  let opens = 0;
  const capture = captureMarketStream({
    openStream: () => ({
      [Symbol.asyncIterator](): AsyncIterator<unknown> {
        opens += 1;
        const error = Object.assign(new Error('token-secret must never be recorded'), { code: 13 });
        return { next: async () => { throw error; } };
      },
    }),
    record: eventsRecorder(events),
    expectedSubscriptions: [],
    acknowledgments: () => [],
    durationMs: 500,
    heartbeatTimeoutMs: 100,
    subscriptionTimeoutMs: 100,
    tickIntervalMs: 5,
    maxReconnects: 5,
    backoffMs: [100],
    signal: controller.signal,
  });
  setTimeout(() => controller.abort(), 18);
  const result = await capture;
  assert.equal(result.reason, 'aborted');
  assert.equal(opens, 1);
  assert.ok(result.ticks >= 1);
  assert.equal(events.at(-1)?.kind, 'stop');
  assert.equal(JSON.stringify(events).includes('token-secret'), false);
  assert.equal((events.find((event) => event.kind === 'disconnect')?.payload as { code: number }).code, 13);
});

test('quiet stream stops at the global deadline and cannot write a late response', async () => {
  const events: CapturedEvent[] = [];
  let resolveNext!: (result: IteratorResult<unknown>) => void;
  const stream: AsyncIterable<unknown> = {
    [Symbol.asyncIterator](): AsyncIterator<unknown> {
      return {
        next: () => new Promise((resolve) => { resolveNext = resolve; }),
        return: () => new Promise<IteratorResult<unknown>>(() => undefined),
      };
    },
  };
  const result = await captureMarketStream({
    openStream: () => stream,
    record: eventsRecorder(events),
    expectedSubscriptions: [],
    acknowledgments: () => [],
    durationMs: 22,
    subscriptionTimeoutMs: 100,
    heartbeatTimeoutMs: 100,
    tickIntervalMs: 5,
    maxReconnects: 0,
  });
  assert.equal(result.reason, 'duration');
  assert.ok(result.ticks >= 3);
  assert.equal(events.at(-1)?.kind, 'stop');
  const eventCount = events.length;
  resolveNext({ done: false, value: { trade: 'late' } });
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(events.length, eventCount);
});

test('record failure aborts immediately before acknowledgment decoding', async () => {
  let decoderCalls = 0;
  let streamSignal: AbortSignal | undefined;
  await assert.rejects(captureMarketStream({
    openStream: (signal) => {
      streamSignal = signal;
      return {
        async *[Symbol.asyncIterator]() {
          yield { trade: { id: 'one' } };
        },
      };
    },
    record: (kind) => {
      if (kind === 'response') throw new Error('disk failed');
    },
    expectedSubscriptions: [],
    acknowledgments: () => {
      decoderCalls += 1;
      return [];
    },
    durationMs: 100,
  }), /disk failed/);
  assert.equal(decoderCalls, 0);
  assert.equal(streamSignal?.aborted, true);
});

test('raw duplicate trades, ACK and ping responses are all retained', async () => {
  const events: CapturedEvent[] = [];
  const duplicate = { trade: { id: 'trade-1', price: 100 } };
  const result = await captureMarketStream({
    openStream: () => ({
      async *[Symbol.asyncIterator]() {
        yield { ack: true };
        yield duplicate;
        yield duplicate;
        yield { ping: 'server-time' };
        await new Promise(() => undefined);
      },
    }),
    record: eventsRecorder(events),
    expectedSubscriptions: ['trades'],
    acknowledgments: (response) => (response as { ack?: boolean }).ack ? [{ key: 'trades', success: true }] : [],
    durationMs: 25,
    subscriptionTimeoutMs: 10,
    heartbeatTimeoutMs: 100,
    tickIntervalMs: 5,
    maxReconnects: 0,
  });
  assert.equal(result.responses, 4);
  const responses = events.filter((event) => event.kind === 'response');
  assert.equal(responses.length, 4);
  assert.deepEqual(responses[1].payload, responses[2].payload);
  assert.equal(events.at(-1)?.kind, 'stop');
});

test('an overdue tick records skipped intervals once instead of emitting a catch-up burst', async () => {
  const events: CapturedEvent[] = [];
  let blocked = false;
  const result = await captureMarketStream({
    openStream: () => ({
      async *[Symbol.asyncIterator]() {
        yield { ack: true };
        await new Promise(() => undefined);
      },
    }),
    record: eventsRecorder(events),
    expectedSubscriptions: ['info'],
    acknowledgments: (response) => {
      if ((response as { ack?: boolean }).ack && !blocked) {
        blocked = true;
        const until = performance.now() + 30;
        while (performance.now() < until) { /* simulate a stalled event loop */ }
        return [{ key: 'info', success: true }];
      }
      return [];
    },
    durationMs: 50,
    subscriptionTimeoutMs: 45,
    heartbeatTimeoutMs: 100,
    tickIntervalMs: 5,
    maxReconnects: 0,
  });
  const tickEvents = events.filter((event) => event.kind === 'tick');
  assert.equal(result.reason, 'duration');
  assert.ok(tickEvents.length <= 6);
  assert.ok(tickEvents.some((event) => (event.payload as { skippedIntervals: number }).skippedIntervals >= 4));
});

test('external abort listeners are released after every frame and after capture', async () => {
  const controller = new AbortController();
  let largestListenerCount = 0;
  let decoded = 0;
  const capture = captureMarketStream({
    openStream: () => ({
      async *[Symbol.asyncIterator]() {
        for (let index = 0; index < 500; index += 1) yield { index };
        await new Promise(() => undefined);
      },
    }),
    record: () => undefined,
    expectedSubscriptions: [],
    acknowledgments: () => {
      decoded += 1;
      largestListenerCount = Math.max(largestListenerCount, getEventListeners(controller.signal, 'abort').length);
      return [];
    },
    durationMs: 1_000,
    subscriptionTimeoutMs: 1_000,
    heartbeatTimeoutMs: 1_000,
    tickIntervalMs: 100,
    maxReconnects: 0,
    signal: controller.signal,
  });
  setTimeout(() => controller.abort(), 15);
  const result = await capture;
  assert.equal(result.reason, 'aborted');
  assert.equal(decoded, 500);
  assert.ok(largestListenerCount <= 1);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});
