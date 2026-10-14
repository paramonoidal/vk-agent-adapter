# Native VK adapter for Moltis

This private repository contains the Rust VK channel used by
[F1uctus/moltis](https://github.com/F1uctus/moltis). The maintained source is
`moltis/`; the host fork includes it directly as `crates/vk`.

The adapter supports VK Bot Long Poll, durable inbox delivery, command buttons,
readable tool events, and incoming/outgoing images, documents and voice recordings.
Host-selected command execution, native subscription agents and working memory
are provided by the Moltis gateway.

```bash
python3 scripts/sync-moltis.py ../moltis --direction check
# Copy an adapter change from a tested host checkout:
python3 scripts/sync-moltis.py ../moltis --direction from-moltis
# Or install the maintained adapter into the host checkout:
python3 scripts/sync-moltis.py ../moltis --direction into-moltis
```

Run `cargo test -p moltis-vk --features metrics,tracing` in the Moltis workspace.
See [COMPATIBILITY.md](COMPATIBILITY.md) for host integration details.

The adapter uses the MIT license in [moltis/LICENSE.md](moltis/LICENSE.md).
