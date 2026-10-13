# VK adapters for OpenClaw and Moltis

This private repository preserves the OpenClaw plugin's Git history and packages
the native Moltis Rust adapter alongside it. Each host uses its own public channel
interfaces; the OpenClaw TypeScript plugin is not loaded into the Rust gateway.

| Host | Source | Integration |
| --- | --- | --- |
| OpenClaw | Root TypeScript package | Existing `vk` manifest, focused plugin SDK imports, setup/doctor/secret contracts |
| Moltis | `moltis/` Rust crate | `ChannelPlugin` and `ChannelEvent`, linked into the Moltis Cargo workspace under the `vk` feature |

The OpenClaw package keeps its upstream package name, manifest ID, configuration
schema, entrypoints, runtime extension paths, optional host peer dependency, and
SDK compatibility declaration. Its build uses native AbortController in the
bundled, pinned vk-io SDK so native fetch cancellation works. It remains private;
install a local packed artifact rather than publishing to public npm:

```bash
npm ci
npm test
npm run check:sdk-imports
npm pack
openclaw plugins install ./openclaw-vk-vk-2026.9.10.tgz
openclaw plugins info vk --json
```

Install any requested OpenClaw host version with `npm install --no-save
--package-lock=false openclaw@<version>` to run its SDK and runtime checks. Use the
host's normal plugin trust and capability options. Existing OpenClaw account and
group policies remain in its manifest; no credentials belong in this repository.

The Moltis fork at https://github.com/F1uctus/moltis contains the same Rust source
directly in `crates/vk`, along with the required additive registry, configuration,
UI, and native-agent integration. A Moltis binary includes native channels at
compile time; an unmodified upstream binary cannot install a Rust crate as an
OpenClaw JavaScript plugin. This repository therefore supplies source, not a
runtime loader or a substitute provider API.

Check or synchronize the source without introducing a private Git dependency or
changing the host's configuration:

```bash
python3 scripts/sync-moltis.py ../moltis --direction check
python3 scripts/sync-moltis.py ../moltis --direction into-moltis
# After editing and testing the fork's native adapter:
python3 scripts/sync-moltis.py ../moltis --direction from-moltis
```

After synchronization, run the Moltis adapter's tests with `cargo test -p
moltis-vk --features metrics,tracing` in the host checkout and its gateway's
affected tests. The helper refuses to delete extra files. Core changes stay in
the Moltis fork; adapter changes are synchronized between both repositories.

The adapters share VK Bot Long Poll, message and attachment APIs. Their host
configuration schemas and command registries are host-specific. Gateway-level
paired execution, subscription CLI routing, Lemmalog and offline transcription
are Moltis fork features; this does not assert those features exist in stock
OpenClaw. Existing imports, MCP tools and upstream channel interfaces remain
available. Upgrade upstream code through the preserved history and rerun the
host compatibility checks before deploying.

OpenClaw source retains its Apache-2.0 license. Moltis adapter source retains its
MIT license in `moltis/LICENSE.md`.
