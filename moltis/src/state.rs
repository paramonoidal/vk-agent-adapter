use {
    crate::{client::VkClient, config::VkAccountConfig},
    std::{
        collections::HashMap,
        sync::{Arc, RwLock},
    },
    tokio_util::sync::CancellationToken,
};

pub(crate) type Accounts = Arc<RwLock<HashMap<String, Arc<Account>>>>;
pub(crate) struct Account {
    pub client: VkClient,
    pub config: RwLock<VkAccountConfig>,
    pub cancel: CancellationToken,
    pub health: RwLock<Option<String>>,
}
