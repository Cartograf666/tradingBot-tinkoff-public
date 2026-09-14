/**
 * Pure economics for a single displayed order-book snapshot.
 *
 * This deliberately models a hypothetical immediate buy and immediate sell against
 * the same snapshot. It is useful for rejecting trades that cannot clear displayed
 * spread, depth and fees; it does not predict a fill, future price or profit.
 */
export interface BookLevel {
  price: number;
  quantityLots: number;
}

export interface DepthBook {
  bids: BookLevel[];
  asks: BookLevel[];
}

export interface BookCostOptions {
  /** Shares in one exchange lot. */
  lotSize: number;
  /** Maximum entry cash in rubles, including entry commission. */
  budgetRub: number;
  /** Decimal fee for each side, for example 0.0005 for 0.05%. */
  commissionRate: number;
  /** Additional adverse price movement per side in basis points. */
  extraSlippageBpsPerSide?: number;
}

export interface AvailableBookCosts {
  status: 'AVAILABLE';
  midpoint: number;
  /** Top-of-book spread divided by midpoint, in basis points. */
  spreadBps: number;
  quantityLots: number;
  quantityShares: number;
  /** VWAP paid across displayed asks, including configured adverse buy slippage. */
  buyVwap: number;
  /** VWAP received across displayed bids, including configured adverse sell slippage. */
  sellVwap: number;
  /** Gross buy consideration after configured adverse buy slippage, before fee. */
  buyNotionalRub: number;
  /** Gross sell consideration after configured adverse sell slippage, before fee. */
  sellNotionalRub: number;
  /** Sum of buy and sell commission in rubles. */
  totalFeesRub: number;
  /** Entry cash less sale proceeds after both commissions. */
  roundTripLossRub: number;
  /** roundTripLossRub divided by gross buy notional, in basis points. */
  roundTripLossBps: number;
  /** Rise from current executable sell VWAP needed to break even, in basis points. */
  breakEvenRiseBps: number;
  /** Gross exit price per share required before the exit commission to break even. */
  requiredExitPrice: number;
  displayedBidLots: number;
  displayedAskLots: number;
}

export interface UnavailableBookCosts {
  status: 'UNAVAILABLE';
  reason: string;
}

export type BookCostAssessment = AvailableBookCosts | UnavailableBookCosts;

interface Execution {
  notional: number;
  vwap: number;
}

function assertFinitePositive(value: unknown, name: string): asserts value is number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be finite and positive`);
  }
}

function finiteComputed(value: number, name: string): number {
  if (!Number.isFinite(value)) throw new Error(`${name} must remain finite`);
  return value;
}

function multiplyFinite(name: string, ...values: number[]): number {
  return finiteComputed(values.reduce((total, value) => total * value, 1), name);
}

function addFinite(name: string, left: number, right: number): number {
  return finiteComputed(left + right, name);
}

function addSafeInteger(name: string, left: number, right: number): number {
  const total = left + right;
  if (!Number.isSafeInteger(total)) throw new Error(`${name} must remain a safe integer`);
  return total;
}

function validateLevels(levels: unknown, side: 'bids' | 'asks'): asserts levels is BookLevel[] {
  if (!Array.isArray(levels) || levels.length === 0) {
    throw new Error(`${side} must contain at least one level`);
  }

  let previousPrice: number | undefined;
  levels.forEach((level, index) => {
    if (!level || typeof level !== 'object') throw new Error(`${side}[${index}] must be an object`);
    assertFinitePositive(level.price, `${side}[${index}].price`);
    if (!Number.isSafeInteger(level.quantityLots) || level.quantityLots <= 0) {
      throw new Error(`${side}[${index}].quantityLots must be a positive safe integer`);
    }
    if (previousPrice !== undefined) {
      const ordered = side === 'bids' ? previousPrice > level.price : previousPrice < level.price;
      if (!ordered) throw new Error(`${side} prices must be strictly ${side === 'bids' ? 'descending' : 'ascending'} and unique`);
    }
    previousPrice = level.price;
  });
}

function adjustedPrice(price: number, multiplier: number): number {
  return multiplyFinite('adjusted price', price, multiplier);
}

function execute(levels: BookLevel[], quantityLots: number, lotSize: number, multiplier: number): Execution | undefined {
  let remainingLots = quantityLots;
  let notional = 0;
  for (const level of levels) {
    if (remainingLots === 0) break;
    const lots = Math.min(remainingLots, level.quantityLots);
    notional = addFinite('execution notional', notional,
      multiplyFinite('level execution notional', adjustedPrice(level.price, multiplier), lots, lotSize));
    remainingLots -= lots;
  }
  if (remainingLots !== 0) return undefined;
  const shares = multiplyFinite('execution share count', quantityLots, lotSize);
  return { notional, vwap: notional / shares };
}

/**
 * Calculates immediately executable round-trip economics from one complete snapshot.
 * Caller is responsible for source, timestamp, consistency and freshness checks.
 */
export function assessBookCosts(book: DepthBook, options: BookCostOptions): BookCostAssessment {
  if (!book || typeof book !== 'object') throw new Error('book must be an object');
  validateLevels(book.bids, 'bids');
  validateLevels(book.asks, 'asks');
  assertFinitePositive(options?.lotSize, 'lotSize');
  if (!Number.isSafeInteger(options.lotSize)) throw new Error('lotSize must be a positive safe integer');
  assertFinitePositive(options.budgetRub, 'budgetRub');
  if (typeof options.commissionRate !== 'number' || !Number.isFinite(options.commissionRate)
    || options.commissionRate < 0 || options.commissionRate >= 1) {
    throw new Error('commissionRate must be finite and in [0, 1)');
  }
  const extraSlippageBps = options.extraSlippageBpsPerSide ?? 0;
  if (!Number.isFinite(extraSlippageBps) || extraSlippageBps < 0) {
    throw new Error('extraSlippageBpsPerSide must be finite and non-negative');
  }
  if (book.bids[0].price >= book.asks[0].price) {
    throw new Error('book must be non-crossed: best bid must be below best ask');
  }

  const commissionMultiplier = finiteComputed(1 + options.commissionRate, 'commission multiplier');
  const buyMultiplier = finiteComputed(1 + extraSlippageBps / 10_000, 'buy slippage multiplier');
  const sellMultiplier = finiteComputed(1 - extraSlippageBps / 10_000, 'sell slippage multiplier');
  if (sellMultiplier <= 0) throw new Error('extraSlippageBpsPerSide leaves no positive sell price');

  // Pick the largest whole-lot order whose displayed asks and entry fee fit the budget.
  let affordableLots = 0;
  let entryCash = 0;
  let exhaustedDisplayedAsks = true;
  let lastAskCashPerLot = 0;
  for (const ask of book.asks) {
    const cashPerLot = multiplyFinite('entry cash per lot', adjustedPrice(ask.price, buyMultiplier), options.lotSize, commissionMultiplier);
    lastAskCashPerLot = cashPerLot;
    const remainingBudget = finiteComputed(options.budgetRub - entryCash, 'remaining budget');
    const affordableAtLevel = Math.floor((remainingBudget / cashPerLot) + 1e-12);
    const lots = Math.min(ask.quantityLots, Math.max(0, affordableAtLevel));
    entryCash = addFinite('entry cash', entryCash, multiplyFinite('entry cash at level', lots, cashPerLot));
    affordableLots = addSafeInteger('affordable lot count', affordableLots, lots);
    if (lots < ask.quantityLots) {
      exhaustedDisplayedAsks = false;
      break;
    }
  }
  if (affordableLots === 0) {
    return { status: 'UNAVAILABLE', reason: 'Budget cannot buy one complete lot including commission and adverse slippage' };
  }
  if (exhaustedDisplayedAsks && options.budgetRub - entryCash >= lastAskCashPerLot) {
    return { status: 'UNAVAILABLE', reason: 'Displayed ask depth is insufficient to determine the budget-sized buy' };
  }

  const buy = execute(book.asks, affordableLots, options.lotSize, buyMultiplier);
  const sell = execute(book.bids, affordableLots, options.lotSize, sellMultiplier);
  if (!buy || !sell) {
    return { status: 'UNAVAILABLE', reason: 'Displayed depth cannot complete the same-size round trip' };
  }

  const quantityShares = multiplyFinite('quantity share count', affordableLots, options.lotSize);
  if (!Number.isSafeInteger(quantityShares)) throw new Error('quantity share count must be a safe integer');
  const buyFee = multiplyFinite('buy fee', buy.notional, options.commissionRate);
  const sellFee = multiplyFinite('sell fee', sell.notional, options.commissionRate);
  const totalFeesRub = addFinite('total fees', buyFee, sellFee);
  const roundTripLossRub = addFinite('round-trip loss', buy.notional + buyFee, -sell.notional + sellFee);
  const requiredExitPrice = finiteComputed(buy.vwap * (1 + options.commissionRate) / (1 - options.commissionRate), 'required exit price');
  const midpoint = finiteComputed((book.bids[0].price + book.asks[0].price) / 2, 'midpoint');
  const spreadBps = finiteComputed(((book.asks[0].price - book.bids[0].price) / midpoint) * 10_000, 'spread bps');
  const roundTripLossBps = finiteComputed((roundTripLossRub / buy.notional) * 10_000, 'round-trip loss bps');
  const breakEvenRiseBps = finiteComputed(((requiredExitPrice / sell.vwap) - 1) * 10_000, 'break-even rise bps');

  return {
    status: 'AVAILABLE',
    midpoint,
    spreadBps,
    quantityLots: affordableLots,
    quantityShares,
    buyVwap: buy.vwap,
    sellVwap: sell.vwap,
    buyNotionalRub: buy.notional,
    sellNotionalRub: sell.notional,
    totalFeesRub,
    roundTripLossRub,
    roundTripLossBps,
    breakEvenRiseBps,
    requiredExitPrice,
    displayedBidLots: book.bids.reduce((total, level) => addSafeInteger('displayed bid lots', total, level.quantityLots), 0),
    displayedAskLots: book.asks.reduce((total, level) => addSafeInteger('displayed ask lots', total, level.quantityLots), 0),
  };
}
