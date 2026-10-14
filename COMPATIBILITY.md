# Host compatibility

`moltis/` contains the native Rust channel compiled under the Moltis host's `vk`
feature. Its maintained host is [F1uctus/moltis](https://github.com/F1uctus/moltis),
where the source is inlined in `crates/vk`. No private Cargo dependency, submodule
or deployment patch is required.

The adapter preserves Moltis channel traits and VK account/message interfaces.
Paired hosts, project selection, native Codex/Claude subscription routing, working
memory and offline transcription remain gateway integrations. The host retains
its provider, MCP and supported import interfaces.

This repository supplies Rust source for compilation into a Moltis workspace.
It has no JavaScript runtime package or OpenClaw plugin loader. Existing Git
history retains the previous implementation for reference.

Use `scripts/sync-moltis.py` with `--direction check`, `into-moltis` or `from-moltis`.
After synchronization, run the adapter's targeted Rust tests in the host checkout.
The helper compares Cargo, source and migration files and refuses extra-file
deletion. Host-specific configuration stays in the host repository.

The native adapter retains the MIT license in `moltis/LICENSE.md`.
