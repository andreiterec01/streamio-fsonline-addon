use std::{ops::Deref, sync::Arc};

use crate::{
    contracts::{Imdb, MovieKey, MovieOrSeriesDataKey, PlayerData},
    service::{
        fsonline_service::{MovieData, VideoServer},
        imdb_service::ImdbService,
    },
};

pub mod fsonline_service;
pub mod imdb_service;
pub mod local_m3u8_player;
pub mod scrappers;

#[derive(Clone)]
pub struct ImdbToVideoServer {
    video_service: VideoServer,
    imdb_service: ImdbService,
}

impl ImdbToVideoServer {
    pub fn new(video_service: VideoServer, imdb_service: ImdbService) -> Self {
        Self {
            imdb_service,
            video_service,
        }
    }

    pub async fn get(&self, imdb_id: Imdb) -> anyhow::Result<Arc<[PlayerData]>> {
        let MovieData {
            movie_name,
            release_year,
        } = self
            .imdb_service
            .get(imdb_id.imdb_id, imdb_id.is_series())
            .await?;
        let data = match imdb_id.series_data {
            Some(data) => MovieOrSeriesDataKey::Series(data),
            None => MovieOrSeriesDataKey::Movie { release_year },
        };
        let r = self
            .video_service
            .get(&MovieKey {
                movie: movie_name,
                data,
            })
            .await?;
        Ok(r)
    }

    pub async fn get_from_server(
        &self,
        imdb_id: Imdb,
        server_name: &str,
    ) -> anyhow::Result<Option<PlayerData>> {
        let r = self.get(imdb_id).await?;

        let player = r
            .iter()
            .find(|p| p.server_name.deref() == server_name)
            .cloned();
        Ok(player)
    }
}
