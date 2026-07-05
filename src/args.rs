use std::path::PathBuf;

#[derive(clap::Parser, Debug)]
pub struct Args {
    #[clap(long, env, default_value_t = 3000)]
    pub port: u16,
    #[clap(long, env)]
    pub ssl_key_path: Option<PathBuf>,
    #[clap(long, env)]
    pub ssl_cert_path: Option<PathBuf>,
    #[clap(long, env)]
    pub headless_browser: bool,
    /// The host where this server is hosted. Used for redirecting subtitles
    #[clap(long, env)]
    pub host: String,
    #[clap(long, env, default_value = "./cache")]
    pub cache_path: PathBuf,
    #[clap(long, env, default_value_t = 1024*1024*5)]
    pub master_cache_size: u64,
    #[clap(long, env, default_value_t = 1024*1024*100)]
    pub memory_segments_cache_size: usize,

    #[clap(long, env, default_value_t = 1024*15)]
    pub file_segments_cache_size_mb: usize,

    #[clap(long, env, default_value_t = 10)]
    pub cache_next_segments: usize,

    #[clap(long, env, default_value_t = 31.)]
    pub max_segment_duration: f32,

    #[clap(long, env, default_value_t = 9)]
    pub timeout_waiting_for_playlist_sec: u64,

    #[clap(long, env, default_value_t = 80.)]
    pub max_segment_duration_after_timeout: f32,
}
