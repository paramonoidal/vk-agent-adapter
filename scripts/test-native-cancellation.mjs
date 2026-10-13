import assert from "node:assert/strict";
import http from "node:http";
import { createRequire } from "node:module";

const original = globalThis.AbortController;
let nativeSignals = 0;
let requests = 0;
globalThis.AbortController = class extends original {
  constructor() {
    super();
    assert.ok(this.signal instanceof globalThis.AbortSignal);
    nativeSignals += 1;
  }
};
const server = http.createServer((request) => {
  assert.equal(request.url, "/method/groups.getById");
  requests += 1;
  // An unresponsive local endpoint must be cancelled by the SDK deadline.
});
try {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const options = {
    token: "test-only", apiVersion: "5.199", apiHeaders: {}, apiTimeout: 250,
    apiBaseUrl: `http://127.0.0.1:${server.address().port}/method`,
  };
  const esm = await import("../node_modules/vk-io/lib/index.mjs");
  const cjs = createRequire(import.meta.url)("../node_modules/vk-io/lib/index.js");
  for (const sdk of [esm, cjs]) {
    const before = performance.now();
    const request = new sdk.APIRequest({ api: { options }, method: "groups.getById" });
    await assert.rejects(request.make(), { name: "AbortError" });
    assert.ok(performance.now() - before < 3000, "SDK deadline must bound an unresponsive endpoint");
  }
  assert.equal(nativeSignals, 2, "Both SDK module formats must use native AbortSignal");
  assert.equal(requests, 2, "Both requests must reach only the local test endpoint");
  console.log("ESM and CommonJS native cancellation passed without contacting VK");
} finally {
  globalThis.AbortController = original;
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}
