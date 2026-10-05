import assert from 'node:assert/strict';
import test from 'node:test';
import { BoundedTimingDistribution, CaptureTiming } from './capture-timing.js';

test('timing histogram clamps nonmonotonic diagnostic durations and remains fixed size', () => {
  const distribution = new BoundedTimingDistribution();
  for (const duration of [-2, 0.25, 0.26, 2, 1001, Number.NaN]) distribution.add(duration);
  assert.deepEqual(distribution.result(), {
    count: 5, meanMs: (0 + 0.25 + 0.26 + 2 + 1001) / 5, maxMs: 1001,
    buckets: [2, 1, 0, 1, 0, 0, 0, 0, 0, 1],
  });
});

test('per-book timing is bounded to twelve configured instruments without retaining frames', () => {
  const ids = Array.from({ length: 20 }, (_, index) => `uid-${index}`);
  const timing = new CaptureTiming(ids);
  timing.observeResponse({ orderbook: { instrumentUid: 'uid-0', bids: ['private'] } }, 1, 3, 2);
  timing.observeResponse({ orderbook: { instrumentUid: 'uid-18', bids: ['private'] } }, 1, 3, 2);
  timing.observeResponse({ orderbook: { instrumentUid: 'unknown', bids: ['private'] } }, 1, 3, 2);
  const result = timing.finish();
  assert.equal(Object.keys(result.bookResponsesByInstrument).length, 12);
  assert.equal(result.allResponses.sdkYieldToDispatchMs.count, 3);
  assert.equal(result.bookResponsesByInstrument['uid-0'].sdkYieldToDispatchMs.count, 1);
  assert.equal(result.bookResponsesByInstrument['uid-11'].sdkYieldToDispatchMs.count, 0);
  assert.equal(JSON.stringify(result).includes('private'), false);
});
