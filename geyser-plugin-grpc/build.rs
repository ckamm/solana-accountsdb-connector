use {
    cargo_lock::Lockfile,
    std::collections::HashSet,
    vergen::{vergen, Config},
};

fn main() -> anyhow::Result<()> {
    // Protobuf
    tonic_build::compile_protos("../proto/geyser.proto")?;

    // Metrics
    vergen(Config::default())?;

    // vergen git version does not looks cool
    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!()
    );

    // Extract Solana version
    let lockfile = Lockfile::load("./Cargo.lock")?;
    println!(
        "cargo:rustc-env=SOLANA_SDK_VERSION={}",
        lockfile
            .packages
            .iter()
            .filter(|pkg| pkg.name.as_str() == "solana-sdk")
            .map(|pkg| pkg.version.to_string())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(",")
    );

    Ok(())
}
