use std::path::PathBuf;

#[derive(clap::Parser)]
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
}
