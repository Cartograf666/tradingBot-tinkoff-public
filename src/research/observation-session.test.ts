import test from 'node:test';
import assert from 'node:assert/strict';
import { nextMainSessionWindow } from './observation-session.js';
import type { ObservationInterval } from './market-observation.js';

const start = Date.parse('2026-09-14T06:00:00Z');
const row = (exchange: string, from = start, to = start + 3600000, type = 'regular_trading_session_main'): ObservationInterval =>
  ({ exchange, type, start: new Date(from).toISOString(), end: new Date(to).toISOString() });

test('session plan selects actual main window across exchanges, excluding dealer and generic hours', () => {
  const instruments = [{ exchange: 'MOEX_A' }, { exchange: 'moex_b' }];
  const intervals = [row('MOEX_A'), row('MOEX_B', start + 300000), row('MOEX_A', start - 86400000, start, 'dealer_holiday_trading_session_main')];
  assert.equal(nextMainSessionWindow(instruments, intervals, start - 60000, 1800000)?.start, new Date(start + 300000).toISOString());
  assert.equal(nextMainSessionWindow(instruments, [row('MOEX_A')], start - 60000, 1800000), null);
  assert.equal(nextMainSessionWindow([{ exchange: 'MOEX_A' }], [row('MOEX_A', start, start + 3600000, 'regular_trading_session')], start, 1800000), null);
});

test('session plan requires the full requested duration plus start margin and handles delayed wakeups', () => {
  const instruments = [{ exchange: 'MOEX_A' }], intervals = [row('MOEX_A'), row('MOEX_A', start + 86400000, start + 90000000)];
  assert.equal(nextMainSessionWindow(instruments, intervals, start + 300000, 1800000)?.start, new Date(start + 300000).toISOString());
  assert.equal(nextMainSessionWindow(instruments, intervals, start + 1800000, 1800000)?.start, new Date(start + 86400000).toISOString());
  assert.throws(() => nextMainSessionWindow(instruments, intervals, start, 0));
});
