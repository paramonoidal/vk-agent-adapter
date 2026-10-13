// Keep cancellation compatible with native fetch and the host's AbortSignal.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = new URL("../node_modules/vk-io/", import.meta.url);
const version = JSON.parse(readFileSync(new URL("package.json", root), "utf8")).version;
if (version !== "4.10.1") throw new Error("Validate VK cancellation before upgrading vk-io");
const replacements = [
  ["lib/index.mjs", "import { AbortController } from 'abort-controller';",
    "const AbortController = globalThis.AbortController; // F1uctus: native cancellation"],
  ["lib/index.js", "var abortController = require('abort-controller');",
    "var abortController = { AbortController: globalThis.AbortController }; // F1uctus: native cancellation"],
];
for (const [relative, before, after] of replacements) {
  const path = fileURLToPath(new URL(relative, root));
  const source = readFileSync(path, "utf8");
  if (source.split(after).length === 2) continue;
  if (source.split(before).length !== 2) throw new Error(`VK cancellation source changed: ${relative}`);
  writeFileSync(path, source.replace(before, after));
}
