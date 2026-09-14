export const TINKOFF_SANDBOX_ENDPOINT = 'sandbox-invest-public-api.tbank.ru:443';
export const TINKOFF_PRODUCTION_ENDPOINT = 'invest-public-api.tbank.ru:443';

export interface TinkoffClientSettings {
  IS_SANDBOX: boolean;
  EXECUTION_MODE: 'PAPER' | 'SANDBOX';
  TINKOFF_API_TOKEN?: string;
  TINKOFF_API_TOKEN_SANDBOX?: string;
}

export function getTinkoffClientOptions(settings: TinkoffClientSettings): { token: string; endpoint: string } {
  const sandbox = settings.IS_SANDBOX || settings.EXECUTION_MODE === 'SANDBOX';
  const productionToken = settings.TINKOFF_API_TOKEN?.trim() || undefined;
  const sandboxToken = settings.TINKOFF_API_TOKEN_SANDBOX?.trim() || undefined;
  const token = sandbox ? sandboxToken ?? productionToken : productionToken;
  if (!token) throw new Error(sandbox ? 'Sandbox API token is required' : 'Production API token is required');
  return { token, endpoint: sandbox ? TINKOFF_SANDBOX_ENDPOINT : TINKOFF_PRODUCTION_ENDPOINT };
}
