use std::{collections::HashMap, ops::Deref};

use crate::{contracts::PlayerOption, service::fsonline_service::VideoAndSubtitles};
pub mod browser_discovery_scrapper;
pub mod file_sun;
pub mod vidmoly;
pub struct PlayerScrappers {
    default_scrapper: PlayerScrapperBox,
    specific_scrapper: HashMap<&'static str, PlayerScrapperBox>,
}

impl PlayerScrappers {
    pub fn new(default_scrapper: impl PlayerScrapper + 'static) -> Self {
        Self {
            default_scrapper: Box::new(default_scrapper),
            specific_scrapper: HashMap::new(),
        }
    }

    pub fn add_scrapper(&mut self, scrapper: impl SpecificScrapper + 'static) {
        let server_name = scrapper.server_name();
        if self
            .specific_scrapper
            .insert(server_name, Box::new(scrapper))
            .is_some()
        {
            panic!("A scrapper for the server {server_name} has added twice");
        }
    }

    pub async fn get_video(&self, player: &PlayerOption) -> anyhow::Result<VideoAndSubtitles> {
        let scrapper = self
            .specific_scrapper
            .get(player.server_name.deref())
            .unwrap_or(&self.default_scrapper);
        scrapper.get_video(&player.iframe_player).await
    }
}

pub type PlayerScrapperBox = Box<dyn PlayerScrapper>;

#[async_trait::async_trait]
pub trait PlayerScrapper: Send + Sync {
    async fn get_video(&self, url: &str) -> anyhow::Result<VideoAndSubtitles>;
}

pub trait SpecificScrapper: PlayerScrapper {
    fn server_name(&self) -> &'static str;
}
