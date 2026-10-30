import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(new URL('../plugins/klaus-bundled/browser/scripts/browser-client.mjs', import.meta.url), 'utf8');
const clientSource = source.replace(/^import \{[\s\S]*?\} from 'klaus:browser-host-transport';/, `
const acquireBrowserHostTransportV1 = () => { throw new Error('A context-bound host transport is required.'); };
const assertBrowserHostTransportV1 = (transport) => {
  if (!transport || !['open', 'send', 'close'].every(name => typeof transport[name] === 'function')) throw new Error('Invalid host transport.');
};
const isBrowserRuntimeProxyV1 = () => false;
const browserHostDefaults = { backend: 'launch' };
const browserDocumentationText = 'Fixture documentation';
`);
const { createBrowserRegistry } = await import(`data:text/javascript;base64,${Buffer.from(clientSource).toString('base64')}`);

function fixture(reply) {
  const requests = [];
  let callbacks;
  const transport = {
    open(_configuration, handlers) { callbacks = handlers; },
    send(bytes, done) {
      const envelope = JSON.parse(new TextDecoder().decode(bytes));
      requests.push(envelope.request);
      const answer = reply(envelope.request, requests.length);
      queueMicrotask(() => callbacks.message(new TextEncoder().encode(JSON.stringify({ id: envelope.id, ...answer }))));
      done?.();
    },
    close() {},
  };
  return { registry: createBrowserRegistry({ transport }), requests };
}
const mention = 'plugin://browser@klaus-bundled?mention=tab-v1&source=extension&browserId=chrome%3Aprofile-1&tabId=42&title=Orders&url=https%3A%2F%2Fshop.test%2Forders';

// A mention resolves and claims through the host before a controllable tab is
// constructed. Its stable browser identity must remain on every later request.
test('tab mention resolution targets the exact external browser on every operation', async () => {
  const { registry, requests } = fixture(request => ({ ok: true, response: request.operation.type === 'resolve_tab'
    ? { value: { browserId: 'chrome:profile-1', tabId: 42, title: 'Orders', url: 'https://shop.test/orders' } }
    : { value: [] } }));
  const browser = await registry.getDefault();
  const tab = await browser.tabs.get({ mention });
  assert.equal(tab.id, '42');
  await tab.goto('https://shop.test/orders');
  assert.deepEqual(requests[0].operation, { type: 'resolve_tab', mention });
  assert.equal(requests[0].permissions.manageTabs, true);
  assert.equal(requests[0].permissions.sessionState, true);
  assert.deepEqual(requests[1].target, { browserId: 'chrome:profile-1', tabId: '42' });
  assert.equal(requests.length, 2, 'the host resolver already claimed the existing tab; no replacement or redundant claim was made');
  await registry.dispose();
});

test('registry mention resolution and legacy string lookup preserve browser and tab identity', async () => {
  const { registry, requests } = fixture(request => ({ ok: true, response: request.operation.type === 'resolve_tab'
    ? { value: { browserId: 'chrome:profile-1', tabId: 42, title: 'Orders', url: 'https://shop.test/orders' } }
    : { value: [] } }));
  const browser = await registry.get({ mention });
  assert.equal(browser.browserId, 'chrome:profile-1');
  const tab = await browser.tabs.get('42');
  await tab.goto('https://shop.test/orders');
  assert.deepEqual(requests[1].target, { browserId: 'chrome:profile-1', tabId: '42' });
  assert.deepEqual(requests[2].target, requests[1].target);
  await registry.dispose();
});

test('external user tabs are listed then claimed with the dedicated ownership operation', async () => {
  const { registry, requests } = fixture(request => ({ ok: true, response: {
    value: request.operation.type === 'tabs' ? [{ id: 42, title: 'Orders', url: 'https://shop.test/orders' }] : {},
  } }));
  const browser = await registry.get('chrome:profile-1');
  const tabs = await browser.user.openTabs();
  assert.equal(tabs[0].browserId, 'chrome:profile-1');
  const tab = await browser.user.claimTab(tabs[0]);
  assert.equal(tab.id, '42');
  assert.deepEqual(requests[0].operation, { type: 'tabs', action: 'user' });
  assert.deepEqual(requests[1].operation, { type: 'claim_tab' });
  assert.deepEqual(requests[1].target, { browserId: 'chrome:profile-1', tabId: '42' });
  await registry.dispose();
});

test('stale mention failure is surfaced without replacement, fallback or navigation', async () => {
  const { registry, requests } = fixture(() => ({ ok: false, error: { code: 'stale_tab', message: 'The referenced browser tab changed.' } }));
  await assert.rejects((await registry.getDefault()).tabs.get({ mention }), { code: 'stale_tab', message: 'The referenced browser tab changed.' });
  assert.equal(requests.length, 1);
  assert.equal(requests[0].operation.type, 'resolve_tab');
  await registry.dispose();
});

test('invalid host resolution cannot construct a fallback tab', async () => {
  for (const value of [{ browserId: 'iab', tabId: 0 }, { browserId: 'iab', tabId: '42' }, { tabId: 42 }, { browserId: 'other-browser', tabId: 42 }]) {
    const { registry, requests } = fixture(() => ({ ok: true, response: { value } }));
    await assert.rejects((await registry.getDefault()).tabs.get({ mention }), { code: 'host_error' });
    assert.equal(requests.length, 1);
    await registry.dispose();
  }
});

test('a built-in mention resolves to the host-scoped iab target', async () => {
  const { registry, requests } = fixture(request => ({ ok: true, response: request.operation.type === 'resolve_tab'
    ? { value: { browserId: 'iab', tabId: 7, title: 'Preview', url: 'http://localhost:3000/' } }
    : { value: [] } }));
  const tab = await (await registry.getDefault()).tabs.get({ mention: 'plugin://browser@klaus-bundled?mention=tab-v1&browserId=conversation-1&tabId=7&title=Preview&url=http%3A%2F%2Flocalhost%3A3000%2F' });
  await tab.goto('http://localhost:3000/');
  assert.deepEqual(requests[1].target, { browserId: 'iab', tabId: '7' });
  await registry.dispose();
});


test('browser registry discovery includes reachable stable external instances', async () => {
  const { registry, requests } = fixture(() => ({ ok: true, response: { value: { browsers: [
    { browserId: 'iab', name: 'Built-in browser', available: true },
    { browserId: 'chrome:profile-1', name: 'Chrome Work', available: true },
    { browserId: 'chrome:offline', name: 'Offline Chrome', available: false },
  ] } } }));
  const browsers = await registry.list();
  assert.deepEqual(browsers.map(browser => browser.browserId), ['iab', 'chrome:profile-1']);
  assert.equal(browsers[1].name, 'Chrome Work');
  assert.deepEqual(requests[0].operation, { type: 'list_connected_browsers', localOnly: true });
  await registry.dispose();
});
