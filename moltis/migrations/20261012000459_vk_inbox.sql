CREATE TABLE IF NOT EXISTS vk_poll_cursor (
    account TEXT PRIMARY KEY,
    ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS vk_inbox (
    account TEXT NOT NULL,
    id TEXT NOT NULL,
    payload TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    PRIMARY KEY(account, id)
);
CREATE INDEX IF NOT EXISTS vk_inbox_pending ON vk_inbox(account, created_at)
    WHERE state != 'done';
