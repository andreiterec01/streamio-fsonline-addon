use std::path::PathBuf;

#[derive(clap::Parser, Debug)]
pub struct Args {
    #[clap(long, env, default_value_t = 3000)]
    pub(crate) port: u16,
    #[clap(long, env)]
    pub(crate) ssl_key_path: Option<PathBuf>,
    #[clap(long, env)]
    pub(crate) ssl_cert_path: Option<PathBuf>,
    #[clap(long, env)]
    pub(crate) headless_browser: bool,
    /// The host where this server is hosted. Used for redirecting subtitles
    #[clap(long, env)]
    pub(crate) host: String,
    #[clap(long, env, default_value = "./movies")]
    pub(crate) cache_path: PathBuf,
    #[clap(long, env, default_value_t = 5)]
    pub(crate) master_cache_size_mb: u64,

    #[clap(long, env, default_value_t = 5)]
    pub(crate) time_cache_size_mb: usize,

    #[clap(long, env, default_value_t = 1024*15)]
    pub(crate) file_segments_cache_size_mb: u64,

    #[clap(long, env, default_value_t = 200.)]
    pub(crate) max_segment_duration: f32,

    #[clap(long, env, default_value_t = 7)]
    pub(crate) timeout_waiting_for_playlist_sec: u64,

    #[clap(long, env, default_value_t = 35.)]
    pub(crate) target_segment_duration: f32,

    #[clap(long, env, default_value_t = 3600*2)]
    pub(crate) metadata_cache_time_to_live: u64,
    #[clap(long, env, default_value_t = 60*20)]
    pub(crate) metadata_cache_time_to_idle: u64,
}
