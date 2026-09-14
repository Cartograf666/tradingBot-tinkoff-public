import assert from 'node:assert/strict';
import test from 'node:test';
import { assessBookCosts, type DepthBook } from './order-book-costs.js';

const oneLevel: DepthBook = {
  bids: [{ price: 99, quantityLots: 10 }],
  asks: [{ price: 101, quantityLots: 10 }],
};

function available(result: ReturnType<typeof assessBookCosts>) {
  assert.equal(result.status, 'AVAILABLE');
  return result;
}

function closeTo(actual: number, expected: number, tolerance = 1e-10) {
  assert.ok(Math.abs(actual - expected) <= tolerance, `${actual} is not within ${tolerance} of ${expected}`);
}

test('calculates an analytic one-level, ten-share-lot round trip', () => {
  const result = available(assessBookCosts(oneLevel, {
    lotSize: 10,
    budgetRub: 1_011.01,
    commissionRate: 0.001,
  }));
  assert.equal(result.quantityLots, 1);
  assert.equal(result.quantityShares, 10);
  assert.equal(result.midpoint, 100);
  assert.equal(result.spreadBps, 200);
  assert.equal(result.buyVwap, 101);
  assert.equal(result.sellVwap, 99);
  assert.equal(result.buyNotionalRub, 1_010);
  assert.equal(result.sellNotionalRub, 990);
  assert.equal(result.totalFeesRub, 2);
  closeTo(result.roundTripLossRub, 22);
  closeTo(result.roundTripLossBps, 22 / 1_010 * 10_000);
  closeTo(result.requiredExitPrice, 101 * 1.001 / 0.999);
  closeTo(result.breakEvenRiseBps, ((101 * 1.001 / 0.999 / 99) - 1) * 10_000);
});

test('walks multiple price levels and reports depth-weighted prices', () => {
  const book: DepthBook = {
    bids: [{ price: 100, quantityLots: 1 }, { price: 99, quantityLots: 2 }],
    asks: [{ price: 101, quantityLots: 1 }, { price: 102, quantityLots: 2 }],
  };
  const snapshot = structuredClone(book);
  const result = available(assessBookCosts(book, { lotSize: 10, budgetRub: 3_100, commissionRate: 0 }));
  assert.equal(result.quantityLots, 3);
  assert.equal(result.buyVwap, 102 - 10 / 30);
  assert.equal(result.sellVwap, 99 + 10 / 30);
  assert.equal(result.displayedBidLots, 3);
  assert.equal(result.displayedAskLots, 3);
  assert.deepEqual(book, snapshot);
});

test('includes adverse slippage independently on both sides', () => {
  const base = available(assessBookCosts(oneLevel, { lotSize: 10, budgetRub: 1_020, commissionRate: 0 }));
  const adverse = available(assessBookCosts(oneLevel, {
    lotSize: 10, budgetRub: 1_030, commissionRate: 0, extraSlippageBpsPerSide: 10,
  }));
  closeTo(adverse.buyVwap, 101.101);
  closeTo(adverse.sellVwap, 98.901);
  assert.ok(adverse.roundTripLossRub > base.roundTripLossRub);
  assert.ok(adverse.breakEvenRiseBps > base.breakEvenRiseBps);
});

test('uses exact budget including commission and refuses a partial lot', () => {
  const exact = available(assessBookCosts(oneLevel, { lotSize: 10, budgetRub: 1_011.01, commissionRate: 0.001 }));
  assert.equal(exact.quantityLots, 1);
  const short = assessBookCosts(oneLevel, { lotSize: 10, budgetRub: 1_011, commissionRate: 0.001 });
  assert.deepEqual(short, { status: 'UNAVAILABLE', reason: 'Budget cannot buy one complete lot including commission and adverse slippage' });
});

test('refuses an incomplete displayed round trip instead of pricing missing bid depth at zero cost', () => {
  const result = assessBookCosts({
    bids: [{ price: 99, quantityLots: 1 }],
    asks: [{ price: 101, quantityLots: 2 }],
  }, { lotSize: 10, budgetRub: 2_020, commissionRate: 0 });
  assert.deepEqual(result, { status: 'UNAVAILABLE', reason: 'Displayed depth cannot complete the same-size round trip' });
});

test('refuses a depleted ask book when the remaining budget could buy another last-price lot', () => {
  const result = assessBookCosts({
    bids: [{ price: 99, quantityLots: 2 }],
    asks: [{ price: 101, quantityLots: 1 }],
  }, { lotSize: 10, budgetRub: 2_020, commissionRate: 0 });
  assert.deepEqual(result, { status: 'UNAVAILABLE', reason: 'Displayed ask depth is insufficient to determine the budget-sized buy' });

  const smallLeftover = available(assessBookCosts({
    bids: [{ price: 99, quantityLots: 1 }],
    asks: [{ price: 101, quantityLots: 1 }],
  }, { lotSize: 10, budgetRub: 1_019.99, commissionRate: 0 }));
  assert.equal(smallLeftover.quantityLots, 1);
});

test('rejects malformed, crossed and financially invalid inputs', () => {
  assert.throws(() => assessBookCosts({ bids: [], asks: [] }, { lotSize: 1, budgetRub: 1, commissionRate: 0 }), /bids/);
  assert.throws(() => assessBookCosts({
    bids: [{ price: 100, quantityLots: 1 }, { price: 100, quantityLots: 1 }], asks: [{ price: 101, quantityLots: 1 }],
  }, { lotSize: 1, budgetRub: 1_000, commissionRate: 0 }), /descending/);
  assert.throws(() => assessBookCosts({
    bids: [{ price: 101, quantityLots: 1 }], asks: [{ price: 101, quantityLots: 1 }],
  }, { lotSize: 1, budgetRub: 1_000, commissionRate: 0 }), /non-crossed/);
  assert.throws(() => assessBookCosts({
    bids: [{ price: Number.NaN, quantityLots: 1 }], asks: [{ price: 101, quantityLots: 1 }],
  }, { lotSize: 1, budgetRub: 1_000, commissionRate: 0 }), /finite and positive/);
  assert.throws(() => assessBookCosts(oneLevel, { lotSize: 1.5, budgetRub: 1_000, commissionRate: 0 }), /integer/);
  assert.throws(() => assessBookCosts(oneLevel, { lotSize: Number.MAX_SAFE_INTEGER + 1, budgetRub: 1_000, commissionRate: 0 }), /safe integer/);
  assert.throws(() => assessBookCosts({
    bids: [{ price: 99, quantityLots: Number.MAX_SAFE_INTEGER + 1 }], asks: [{ price: 101, quantityLots: 1 }],
  }, { lotSize: 1, budgetRub: 1_000, commissionRate: 0 }), /safe integer/);
  assert.throws(() => assessBookCosts({
    bids: [{ price: 1, quantityLots: 1 }], asks: [{ price: Number.MAX_VALUE, quantityLots: 1 }],
  }, { lotSize: 2, budgetRub: Number.MAX_VALUE, commissionRate: 0 }), /finite/);
  assert.throws(() => assessBookCosts(oneLevel, { lotSize: 1, budgetRub: 1_000, commissionRate: 1 }), /\[0, 1\)/);
  assert.throws(() => assessBookCosts(oneLevel, { lotSize: 1, budgetRub: 1_000, commissionRate: 0, extraSlippageBpsPerSide: -1 }), /non-negative/);
});
