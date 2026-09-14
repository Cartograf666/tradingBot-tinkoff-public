import assert from 'node:assert/strict';
import test from 'node:test';
import { X509Certificate } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { getTinkoffClientOptions, TINKOFF_PRODUCTION_ENDPOINT, TINKOFF_SANDBOX_ENDPOINT } from './tinkoff-client.js';

test('broker CA is the verified public official root, valid and self-signed without private key material', () => {
  const pem = readFileSync('certs/russian-trusted-root-ca.pem', 'utf8');
  assert.doesNotMatch(pem, /PRIVATE KEY/);
  const certificate = new X509Certificate(pem);
  assert.equal(certificate.fingerprint256, 'D2:6D:2D:02:31:B7:C3:9F:92:CC:73:85:12:BA:54:10:35:19:E4:40:5D:68:B5:BD:70:3E:97:88:CA:8E:CF:31');
  assert.equal(certificate.ca, true);
  assert.equal(certificate.verify(certificate.publicKey), true);
  assert.ok(Date.parse(certificate.validFrom) <= Date.now() && Date.now() < Date.parse(certificate.validTo));
});

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
