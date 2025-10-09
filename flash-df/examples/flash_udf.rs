#[cfg(not(feature = "datafusion"))]
fn main() {
    eprintln!(
        "This example requires the `datafusion` feature. Run:\n  cargo run -p flash-df --example flash_udf --features datafusion -- ..."
    );
}

#[cfg(feature = "datafusion")]
use datafusion::arrow::util::pretty::print_batches;
#[cfg(feature = "datafusion")]
fn main() -> anyhow::Result<()> {
    use std::env;
    use std::path::PathBuf;

    use flash_df::{FlashDistributedJob, FlashJobConfig};
    use flash_lib::CombineParams;

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: cargo run -p flash-df --example flash_udf --features datafusion -- \\n             <forward.fq> <reverse.fq> [limit]\n\
             Example: cargo run -p flash-df --example flash_udf --features datafusion -- \\n             input1.fq input2.fq 5"
        );
        std::process::exit(2);
    }

    let forward = PathBuf::from(&args[1]);
    let reverse = PathBuf::from(&args[2]);
    let limit: usize = args.get(3).map(|s| s.parse().unwrap_or(5)).unwrap_or(5);

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let job = FlashDistributedJob::new(
            FlashJobConfig::new(&forward, &reverse, std::env::temp_dir(), "df_udf"),
            CombineParams::default(),
        );

        let ctx = job.session_context().await?;
        job.register_fastq_sources(&ctx).await?;

        let plans = job.build_flash_plans(&ctx).await?;

        println!("=== combined (first {limit}) ===");
        print_plan(&ctx, &plans.combined, limit).await?;

        println!("=== not combined left (first {limit}) ===");
        print_plan(&ctx, &plans.not_combined_left, limit).await?;

        println!("=== not combined right (first {limit}) ===");
        print_plan(&ctx, &plans.not_combined_right, limit).await?;

        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}

#[cfg(feature = "datafusion")]
async fn print_plan(
    ctx: &datafusion::prelude::SessionContext,
    plan: &datafusion::logical_expr::LogicalPlan,
    limit: usize,
) -> anyhow::Result<()> {
    let df = ctx.execute_logical_plan(plan.clone()).await?;
    let df = df.limit(0, Some(limit))?;
    let batches = df.collect().await?;
    print_batches(&batches)?;
    Ok(())
}
