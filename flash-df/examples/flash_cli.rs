#[cfg(not(feature = "datafusion"))]
fn main() {
    eprintln!(
        "This example requires the `datafusion` feature. Run:\n  cargo run -p flash-df --example flash_cli --features datafusion -- ..."
    );
}

#[cfg(feature = "datafusion")]
fn main() -> anyhow::Result<()> {
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    use flash_df::{FlashDistributedJob, FlashJobConfig};
    use flash_lib::CombineParams;

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: cargo run -p flash-df --example flash_cli --features datafusion -- \
             <forward.fq> <reverse.fq> [output_dir] [output_prefix]\n\
             Example: cargo run -p flash-df --example flash_cli --features datafusion -- \
             input1.fq input2.fq ./out flash"
        );
        std::process::exit(2);
    }

    let forward = PathBuf::from(&args[1]);
    let reverse = PathBuf::from(&args[2]);
    let output_dir = args
        .get(3)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let output_prefix = args.get(4).cloned().unwrap_or_else(|| "flash".to_string());

    fs::create_dir_all(&output_dir)?;

    let job = FlashDistributedJob::new(
        FlashJobConfig::new(&forward, &reverse, &output_dir, &output_prefix),
        CombineParams::default(),
    );

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        job.execute_datafusion().await?;
        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}
