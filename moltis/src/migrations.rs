use {
    moltis_channels::{Error, Result},
    sqlx::SqlitePool,
};

pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .set_ignore_missing(true)
        .run(pool)
        .await
        .map_err(|error| Error::external("VK migrations", error))
}
