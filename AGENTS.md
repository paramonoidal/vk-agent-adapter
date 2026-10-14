# Working on the native VK adapter

The Rust adapter lives in `moltis/` and is maintained alongside `crates/vk` in
the Moltis fork. Its Cargo manifest inherits the host workspace dependencies.
Keep the Moltis channel traits, account configuration, durable inbox, message
keyboard, attachment handling and cancellation behavior compatible.

Use `scripts/sync-moltis.py` to check or synchronize source. Run adapter tests in
the host Moltis checkout after a change. The sync helper refuses to delete extra
adapter files; review intentional deletions explicitly. Core gateway changes
belong in the host fork. Keep credentials and runtime state outside Git.

Use the host's pinned Rust formatter. Make conventional commits without
attribution trailers. Do not publish releases or enable automation without
explicit user authorization.
