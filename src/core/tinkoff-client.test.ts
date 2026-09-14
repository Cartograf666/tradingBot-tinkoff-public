import assert from 'node:assert/strict';
import test from 'node:test';
import { getTinkoffClientOptions, TINKOFF_PRODUCTION_ENDPOINT, TINKOFF_SANDBOX_ENDPOINT } from './tinkoff-client.js';

test('dedicated sandbox token takes precedence and legacy token remains a fallback', () => {
  assert.deepEqual(getTinkoffClientOptions({ IS_SANDBOX: true, EXECUTION_MODE: 'PAPER', TINKOFF_API_TOKEN: 'legacy', TINKOFF_API_TOKEN_SANDBOX: 'sandbox' }), { token: 'sandbox', endpoint: TINKOFF_SANDBOX_ENDPOINT });
  assert.deepEqual(getTinkoffClientOptions({ IS_SANDBOX: false, EXECUTION_MODE: 'SANDBOX', TINKOFF_API_TOKEN: 'legacy' }), { token: 'legacy', endpoint: TINKOFF_SANDBOX_ENDPOINT });
});

test('production never selects the sandbox token and fails closed without production credentials', () => {
  assert.deepEqual(getTinkoffClientOptions({ IS_SANDBOX: false, EXECUTION_MODE: 'PAPER', TINKOFF_API_TOKEN: 'production', TINKOFF_API_TOKEN_SANDBOX: 'sandbox' }), { token: 'production', endpoint: TINKOFF_PRODUCTION_ENDPOINT });
  assert.throws(() => getTinkoffClientOptions({ IS_SANDBOX: false, EXECUTION_MODE: 'PAPER', TINKOFF_API_TOKEN_SANDBOX: 'sandbox' }), /Production API token/);
  assert.throws(() => getTinkoffClientOptions({ IS_SANDBOX: true, EXECUTION_MODE: 'PAPER' }), /Sandbox API token/);
});

test('blank dedicated token falls back in sandbox and never becomes a production credential', () => {
  assert.deepEqual(getTinkoffClientOptions({ IS_SANDBOX: true, EXECUTION_MODE: 'PAPER', TINKOFF_API_TOKEN: ' legacy ', TINKOFF_API_TOKEN_SANDBOX: '  ' }), { token: 'legacy', endpoint: TINKOFF_SANDBOX_ENDPOINT });
  assert.throws(() => getTinkoffClientOptions({ IS_SANDBOX: false, EXECUTION_MODE: 'PAPER', TINKOFF_API_TOKEN: ' ', TINKOFF_API_TOKEN_SANDBOX: 'sandbox' }), /Production API token/);
});
