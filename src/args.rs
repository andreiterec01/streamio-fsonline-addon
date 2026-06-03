use std::path::PathBuf;

#[derive(clap::Parser)]
pub struct Args {
    #[clap(long, env, default_value_t = 3000)]
    pub port: u16,
    #[clap(long, env)]
    pub ssl_key_path: PathBuf,
    #[clap(long, env)]
    pub ssl_cert_path: PathBuf,
    #[clap(long, env)]
    pub headless_browser: bool,
}
