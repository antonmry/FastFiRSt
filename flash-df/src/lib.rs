//! DataFusion and Ballista integration layer for the FLASH port.
//!
//! The goal of this crate is to expose helpers that register the FASTQ inputs
//! handled by `flash-lib` as DataFusion data sources and build the logical plan
//! necessary to reproduce FLASH's merging behaviour.  It also sketches the
//! entry points needed to submit the job to a Ballista cluster.  The heavy
//! lifting is still carried out by `flash-lib`; this crate focuses on the
//! distributed execution wiring.

use anyhow::Result;
#[cfg(feature = "datafusion")]
use flash_lib::FastqRecord;
#[cfg(feature = "datafusion")]
use flash_lib::combine_pair_from_strs;
use flash_lib::{CombineParams, merge_fastq_files};

#[cfg(feature = "datafusion")]
use anyhow::anyhow;
use std::path::{Path, PathBuf};
#[cfg(feature = "datafusion")]
use std::{any::Any, sync::Arc};

#[cfg(feature = "datafusion")]
use async_trait::async_trait;
#[cfg(feature = "datafusion")]
use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, StringArray, StringBuilder};
#[cfg(feature = "datafusion")]
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
#[cfg(feature = "datafusion")]
#[cfg(feature = "datafusion")]
use datafusion::arrow::record_batch::RecordBatch;
#[cfg(feature = "datafusion")]
use datafusion::catalog::Session;
#[cfg(feature = "datafusion")]
use datafusion::common::cast::as_string_array;
#[cfg(feature = "datafusion")]
use datafusion::dataframe::DataFrame;
#[cfg(feature = "datafusion")]
use datafusion::datasource::{TableProvider, TableType};
#[cfg(feature = "datafusion")]
use datafusion::error::{DataFusionError, Result as DFResult};
#[cfg(feature = "datafusion")]
use datafusion::execution::context::{SessionContext, TaskContext};
#[cfg(feature = "datafusion")]
use datafusion::logical_expr::ColumnarValue;
#[cfg(feature = "datafusion")]
use datafusion::logical_expr::{
    Expr, LogicalPlan, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col,
};
#[cfg(feature = "datafusion")]
use datafusion::physical_expr::EquivalenceProperties;
#[cfg(feature = "datafusion")]
use datafusion::physical_plan::execution_plan::{DisplayAs, DisplayFormatType};
#[cfg(feature = "datafusion")]
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
#[cfg(feature = "datafusion")]
use datafusion::physical_plan::{
    ExecutionMode, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
#[cfg(feature = "datafusion")]
use datafusion::prelude::SessionConfig;
#[cfg(feature = "datafusion")]
use flash_lib::FastqPairReader;
#[cfg(feature = "datafusion")]
use futures::{Stream, StreamExt};
#[cfg(feature = "datafusion")]
use std::fmt;
#[cfg(feature = "datafusion")]
use std::fs::{self, File};
#[cfg(feature = "datafusion")]
use std::io::{BufWriter, Write};
#[cfg(feature = "datafusion")]
use std::pin::Pin;
#[cfg(feature = "datafusion")]
use std::task::{Context, Poll};

#[cfg(feature = "datafusion")]
const FLASH_COMBINED_TAG_UDF: &str = "flash_combined_tag";
#[cfg(feature = "datafusion")]
const FLASH_COMBINED_SEQ_UDF: &str = "flash_combined_seq";
#[cfg(feature = "datafusion")]
const FLASH_COMBINED_QUAL_UDF: &str = "flash_combined_qual";
#[cfg(feature = "datafusion")]
const FLASH_IS_COMBINED_UDF: &str = "flash_is_combined";
#[cfg(feature = "datafusion")]
const IS_COMBINED_COL: &str = "is_combined";
#[cfg(feature = "datafusion")]
const COMBINED_TAG_COL: &str = "combined_tag";
#[cfg(feature = "datafusion")]
const COMBINED_SEQ_COL: &str = "combined_seq";
#[cfg(feature = "datafusion")]
const COMBINED_QUAL_COL: &str = "combined_qual";

/// Configuration for a FLASH job, regardless of which execution backend is
/// used.
#[derive(Debug, Clone)]
pub struct FlashJobConfig {
    /// Forward FASTQ file.
    pub forward: PathBuf,
    /// Reverse FASTQ file.
    pub reverse: PathBuf,
    /// Output directory to receive the three FASTQ artefacts.
    pub output_dir: PathBuf,
    /// Output prefix (defaults to `out` in the CLI).
    pub output_prefix: String,
    /// Optional batch size hint for streaming scans.
    pub batch_size: Option<usize>,
}

impl FlashJobConfig {
    /// Build a new configuration description.
    pub fn new(
        forward: impl AsRef<Path>,
        reverse: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        output_prefix: impl Into<String>,
    ) -> Self {
        Self {
            forward: forward.as_ref().to_path_buf(),
            reverse: reverse.as_ref().to_path_buf(),
            output_dir: output_dir.as_ref().to_path_buf(),
            output_prefix: output_prefix.into(),
            batch_size: None,
        }
    }

    /// Override the batch size used when streaming FASTQ rows.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }
}

/// Wrapper that couples job configuration with the merge parameters.
#[derive(Debug, Clone)]
pub struct FlashDistributedJob {
    config: FlashJobConfig,
    params: CombineParams,
}

impl FlashDistributedJob {
    /// Compose a distributed job description.
    pub fn new(config: FlashJobConfig, params: CombineParams) -> Self {
        Self { config, params }
    }

    /// Borrow the configuration component.
    pub fn config(&self) -> &FlashJobConfig {
        &self.config
    }

    /// Borrow the merge parameters.
    pub fn params(&self) -> &CombineParams {
        &self.params
    }

    /// Execute the job locally using the reference implementation in
    /// `flash-lib`.  This is handy for testing the distributed pipeline logic.
    pub fn execute_local(&self) -> Result<()> {
        merge_fastq_files(
            &self.config.forward,
            &self.config.reverse,
            &self.config.output_dir,
            &self.config.output_prefix,
            &self.params,
        )
    }
}

#[cfg(feature = "datafusion")]
const FASTQ_TABLE_NAME: &str = "flash_pairs";
#[cfg(feature = "datafusion")]
const DEFAULT_BATCH_SIZE: usize = 2048;

#[cfg(feature = "datafusion")]
fn to_df_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

#[cfg(feature = "datafusion")]
pub struct FastqTableProvider {
    forward: PathBuf,
    reverse: PathBuf,
    phred_offset: u8,
    schema: SchemaRef,
    batch_size: usize,
}

#[cfg(feature = "datafusion")]
impl FastqTableProvider {
    pub fn new(forward: PathBuf, reverse: PathBuf, phred_offset: u8, batch_size: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, false),
            Field::new("seq1", DataType::Utf8, false),
            Field::new("qual1", DataType::Utf8, false),
            Field::new("tag2", DataType::Utf8, false),
            Field::new("seq2", DataType::Utf8, false),
            Field::new("qual2", DataType::Utf8, false),
        ]));
        Self {
            forward,
            reverse,
            phred_offset,
            schema,
            batch_size,
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(feature = "datafusion")]
#[async_trait]
impl TableProvider for FastqTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let exec = FastqScanExec::new(
            self.forward.clone(),
            self.reverse.clone(),
            self.schema.clone(),
            projection.cloned(),
            limit,
            self.phred_offset,
            self.batch_size,
        );
        Ok(Arc::new(exec))
    }
}

#[cfg(feature = "datafusion")]
#[derive(Debug)]
struct FastqScanExec {
    forward: PathBuf,
    reverse: PathBuf,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    phred_offset: u8,
    batch_size: usize,
    properties: PlanProperties,
}

#[cfg(feature = "datafusion")]
impl FastqScanExec {
    fn new(
        forward: PathBuf,
        reverse: PathBuf,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        phred_offset: u8,
        batch_size: usize,
    ) -> Self {
        let projected_schema = match &projection {
            Some(cols) => Arc::new(Schema::new(
                cols.iter()
                    .map(|i| schema.field(*i).clone())
                    .collect::<Vec<_>>(),
            )),
            None => schema.clone(),
        };

        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            forward,
            reverse,
            schema,
            projected_schema,
            projection,
            limit,
            phred_offset,
            batch_size,
            properties,
        }
    }
}

#[cfg(feature = "datafusion")]
impl ExecutionPlan for FastqScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "FastqScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "FastqScanExec does not support children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "FastqScanExec supports a single partition".to_string(),
            ));
        }

        let reader = FastqPairReader::from_paths(&self.forward, &self.reverse, self.phred_offset)
            .map_err(to_df_error)?;

        let stream = FastqScanStream::new(
            reader,
            self.schema.clone(),
            self.projection.clone(),
            self.limit,
            self.batch_size,
            self.phred_offset,
        );

        let stream = RecordBatchStreamAdapter::new(self.projected_schema.clone(), stream);
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(self.projected_schema.as_ref()))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

#[cfg(feature = "datafusion")]
impl DisplayAs for FastqScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "FastqScanExec"),
            DisplayFormatType::Verbose => write!(
                f,
                "FastqScanExec: forward={}, reverse={}",
                self.forward.display(),
                self.reverse.display()
            ),
        }
    }
}

#[cfg(feature = "datafusion")]
struct FastqScanStream {
    reader: FastqPairReader,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    batch_size: usize,
    rows_emitted: usize,
    finished: bool,
    builder: FastqBatchBuilder,
}

#[cfg(feature = "datafusion")]
impl FastqScanStream {
    fn new(
        reader: FastqPairReader,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        batch_size: usize,
        phred_offset: u8,
    ) -> Self {
        Self {
            reader,
            schema,
            projection,
            limit,
            batch_size,
            rows_emitted: 0,
            finished: false,
            builder: FastqBatchBuilder::new(phred_offset),
        }
    }
}

#[cfg(feature = "datafusion")]
impl Stream for FastqScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let mut reached_limit = false;

        while self.builder.len() < self.batch_size {
            if let Some(limit) = self.limit {
                if self.rows_emitted + self.builder.len() >= limit {
                    reached_limit = true;
                    break;
                }
            }

            match self.reader.next_pair() {
                Ok(Some((r1, r2))) => {
                    self.builder.append(&r1, &r2);
                }
                Ok(None) => {
                    self.finished = true;
                    break;
                }
                Err(err) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(to_df_error(err))));
                }
            }
        }

        if self.builder.is_empty() {
            self.finished = true;
            return Poll::Ready(None);
        }

        let schema = self.schema.clone();
        let projection = self.projection.clone();

        match self.builder.take_batch(&schema, projection) {
            Ok(batch) => {
                self.rows_emitted += batch.num_rows();
                if reached_limit {
                    self.finished = true;
                } else if let Some(limit) = self.limit {
                    if self.rows_emitted >= limit {
                        self.finished = true;
                    }
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Err(err) => {
                self.finished = true;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

#[cfg(feature = "datafusion")]
struct FastqBatchBuilder {
    tag1: StringBuilder,
    seq1: StringBuilder,
    qual1: StringBuilder,
    tag2: StringBuilder,
    seq2: StringBuilder,
    qual2: StringBuilder,
    rows: usize,
    phred_offset: u8,
}

#[cfg(feature = "datafusion")]
impl FastqBatchBuilder {
    fn new(phred_offset: u8) -> Self {
        Self {
            tag1: StringBuilder::new(),
            seq1: StringBuilder::new(),
            qual1: StringBuilder::new(),
            tag2: StringBuilder::new(),
            seq2: StringBuilder::new(),
            qual2: StringBuilder::new(),
            rows: 0,
            phred_offset,
        }
    }

    fn append(&mut self, r1: &FastqRecord, r2: &FastqRecord) {
        let seq1_str = r1.seq_string();
        let qual1_str = r1.qual_string(self.phred_offset);
        let seq2_str = r2.seq_string();
        let qual2_str = r2.qual_string(self.phred_offset);

        self.tag1.append_value(r1.tag());
        self.seq1.append_value(&seq1_str);
        self.qual1.append_value(&qual1_str);

        self.tag2.append_value(r2.tag());
        self.seq2.append_value(&seq2_str);
        self.qual2.append_value(&qual2_str);

        self.rows += 1;
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn is_empty(&self) -> bool {
        self.rows == 0
    }

    fn take_batch(
        &mut self,
        schema: &SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> DFResult<RecordBatch> {
        let mut tag1_builder = std::mem::replace(&mut self.tag1, StringBuilder::new());
        let mut seq1_builder = std::mem::replace(&mut self.seq1, StringBuilder::new());
        let mut qual1_builder = std::mem::replace(&mut self.qual1, StringBuilder::new());
        let mut tag2_builder = std::mem::replace(&mut self.tag2, StringBuilder::new());
        let mut seq2_builder = std::mem::replace(&mut self.seq2, StringBuilder::new());
        let mut qual2_builder = std::mem::replace(&mut self.qual2, StringBuilder::new());

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(tag1_builder.finish()),
            Arc::new(seq1_builder.finish()),
            Arc::new(qual1_builder.finish()),
            Arc::new(tag2_builder.finish()),
            Arc::new(seq2_builder.finish()),
            Arc::new(qual2_builder.finish()),
        ];

        self.rows = 0;

        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;
        if let Some(projection) = projection {
            batch
                .project(&projection)
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        } else {
            Ok(batch)
        }
    }
}

#[cfg(feature = "datafusion")]
impl FlashDistributedJob {
    /// Construct a `SessionContext` configured for FLASH workloads.
    pub async fn session_context(&self) -> Result<SessionContext> {
        let mut cfg = SessionConfig::new();
        cfg = cfg.with_target_partitions(1);
        Ok(SessionContext::new_with_config(cfg))
    }

    pub fn fastq_table_provider(&self) -> FastqTableProvider {
        FastqTableProvider::new(
            self.config.forward.clone(),
            self.config.reverse.clone(),
            self.params.phred_offset,
            self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
        )
    }

    /// Register the FASTQ sources backing this job.
    pub async fn register_fastq_sources(&self, ctx: &SessionContext) -> Result<()> {
        let provider = Arc::new(self.fastq_table_provider());
        ctx.register_table(FASTQ_TABLE_NAME, provider)
            .map_err(|e| anyhow!(e.to_string()))?;
        self.ensure_flash_udfs(ctx)?;
        Ok(())
    }

    /// Build the logical plan that mirrors FLASH's merge stages by annotating
    /// each FASTQ pair with the FLASH UDF outputs.
    pub async fn build_logical_plan(&self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let df = self.annotated_fastq_df(ctx).await?;
        df.into_optimized_plan().map_err(|e| anyhow!(e.to_string()))
    }

    /// Build logical plans that produce combined reads and the two not-combined
    /// FASTQ outputs, emulating FLASH's stages.
    pub async fn build_flash_plans(&self, ctx: &SessionContext) -> Result<FlashPlans> {
        let df = self.annotated_fastq_df(ctx).await?;

        let combined_df = df.clone().filter(col(IS_COMBINED_COL))?.select(vec![
            col(COMBINED_TAG_COL).alias("tag"),
            col(COMBINED_SEQ_COL).alias("seq"),
            col(COMBINED_QUAL_COL).alias("qual"),
        ])?;

        let not_expr = Expr::Not(Box::new(col(IS_COMBINED_COL)));

        let not_left_df = df.clone().filter(not_expr.clone())?.select(vec![
            col("tag1").alias("tag"),
            col("seq1").alias("seq"),
            col("qual1").alias("qual"),
        ])?;

        let not_right_df = df.filter(not_expr)?.select(vec![
            col("tag2").alias("tag"),
            col("seq2").alias("seq"),
            col("qual2").alias("qual"),
        ])?;

        Ok(FlashPlans {
            combined: combined_df.into_optimized_plan()?,
            not_combined_left: not_left_df.into_optimized_plan()?,
            not_combined_right: not_right_df.into_optimized_plan()?,
        })
    }

    /// Execute the FLASH pipeline through DataFusion and materialise the three
    /// FASTQ outputs.
    pub async fn execute_datafusion(&self) -> Result<()> {
        let ctx = self.session_context().await?;
        self.register_fastq_sources(&ctx).await?;
        let plans = self.build_flash_plans(&ctx).await?;
        self.materialize_plans(&ctx, &plans).await
    }

    async fn annotated_fastq_df(&self, ctx: &SessionContext) -> Result<DataFrame> {
        self.ensure_flash_udfs(ctx)?;

        let df = ctx
            .table(FASTQ_TABLE_NAME)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;

        df.select(vec![
            col("tag1"),
            col("seq1"),
            col("qual1"),
            col("tag2"),
            col("seq2"),
            col("qual2"),
            flash_bool_udf(self.params.clone())
                .call(flash_udf_args())
                .alias(IS_COMBINED_COL),
            flash_string_udf(
                FLASH_COMBINED_TAG_UDF,
                FlashStringField::Tag,
                self.params.clone(),
            )
            .call(flash_udf_args())
            .alias(COMBINED_TAG_COL),
            flash_string_udf(
                FLASH_COMBINED_SEQ_UDF,
                FlashStringField::Seq,
                self.params.clone(),
            )
            .call(flash_udf_args())
            .alias(COMBINED_SEQ_COL),
            flash_string_udf(
                FLASH_COMBINED_QUAL_UDF,
                FlashStringField::Qual,
                self.params.clone(),
            )
            .call(flash_udf_args())
            .alias(COMBINED_QUAL_COL),
        ])
        .map_err(|e| anyhow!(e.to_string()))
    }

    async fn materialize_plans(&self, ctx: &SessionContext, plans: &FlashPlans) -> Result<()> {
        fs::create_dir_all(&self.config.output_dir).map_err(|e| anyhow!(e.to_string()))?;

        let (combined_path, not1_path, not2_path) = self.output_paths();

        self.write_plan_stream(ctx, &plans.combined, &combined_path, 0, 1, 2)
            .await?;
        self.write_plan_stream(ctx, &plans.not_combined_left, &not1_path, 0, 1, 2)
            .await?;
        self.write_plan_stream(ctx, &plans.not_combined_right, &not2_path, 0, 1, 2)
            .await?;

        Ok(())
    }

    async fn write_plan_stream(
        &self,
        ctx: &SessionContext,
        plan: &LogicalPlan,
        path: &Path,
        tag_idx: usize,
        seq_idx: usize,
        qual_idx: usize,
    ) -> Result<()> {
        let df = ctx
            .execute_logical_plan(plan.clone())
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        let mut stream = df
            .execute_stream()
            .await
            .map_err(|e| anyhow!(e.to_string()))?;

        let file = File::create(path).map_err(|e| anyhow!(e.to_string()))?;
        let mut writer = BufWriter::new(file);

        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| anyhow!(e.to_string()))?;
            self.write_fastq_batch(&mut writer, &batch, tag_idx, seq_idx, qual_idx)?;
        }

        writer.flush().map_err(|e| anyhow!(e.to_string()))?;
        Ok(())
    }

    fn write_fastq_batch<W: Write>(
        &self,
        writer: &mut W,
        batch: &RecordBatch,
        tag_idx: usize,
        seq_idx: usize,
        qual_idx: usize,
    ) -> Result<()> {
        let tags = batch
            .column(tag_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("expected UTF-8 tag column"))?;
        let seqs = batch
            .column(seq_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("expected UTF-8 sequence column"))?;
        let quals = batch
            .column(qual_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("expected UTF-8 quality column"))?;

        for row in 0..batch.num_rows() {
            if tags.is_null(row) || seqs.is_null(row) || quals.is_null(row) {
                continue;
            }

            writeln!(writer, "{}", tags.value(row)).map_err(|e| anyhow!(e.to_string()))?;
            writeln!(writer, "{}", seqs.value(row)).map_err(|e| anyhow!(e.to_string()))?;
            writer
                .write_all(b"+\n")
                .map_err(|e| anyhow!(e.to_string()))?;
            writeln!(writer, "{}", quals.value(row)).map_err(|e| anyhow!(e.to_string()))?;
        }

        Ok(())
    }

    fn output_paths(&self) -> (PathBuf, PathBuf, PathBuf) {
        let prefix = &self.config.output_prefix;
        let dir = &self.config.output_dir;
        (
            dir.join(format!("{}.extendedFrags.fastq", prefix)),
            dir.join(format!("{}.notCombined_1.fastq", prefix)),
            dir.join(format!("{}.notCombined_2.fastq", prefix)),
        )
    }

    fn ensure_flash_udfs(&self, ctx: &SessionContext) -> Result<()> {
        ctx.register_udf(flash_string_udf(
            FLASH_COMBINED_TAG_UDF,
            FlashStringField::Tag,
            self.params.clone(),
        ));
        ctx.register_udf(flash_string_udf(
            FLASH_COMBINED_SEQ_UDF,
            FlashStringField::Seq,
            self.params.clone(),
        ));
        ctx.register_udf(flash_string_udf(
            FLASH_COMBINED_QUAL_UDF,
            FlashStringField::Qual,
            self.params.clone(),
        ));
        ctx.register_udf(flash_bool_udf(self.params.clone()));
        Ok(())
    }
}

#[cfg(feature = "datafusion")]
fn flash_udf_args() -> Vec<Expr> {
    vec![
        col("tag1"),
        col("seq1"),
        col("qual1"),
        col("tag2"),
        col("seq2"),
        col("qual2"),
    ]
}

#[cfg(feature = "datafusion")]
#[derive(Clone, Copy, Debug)]
enum FlashStringField {
    Tag,
    Seq,
    Qual,
}

#[cfg(feature = "datafusion")]
fn flash_string_udf(
    name: &'static str,
    field: FlashStringField,
    params: CombineParams,
) -> ScalarUDF {
    ScalarUDF::from(FlashStringUdf::new(name, field, params))
}

#[cfg(feature = "datafusion")]
fn flash_bool_udf(params: CombineParams) -> ScalarUDF {
    ScalarUDF::from(FlashBoolUdf::new(params))
}

#[cfg(feature = "datafusion")]
#[derive(Debug)]
struct FlashStringUdf {
    name: &'static str,
    field: FlashStringField,
    params: Arc<CombineParams>,
    signature: Signature,
}

#[cfg(feature = "datafusion")]
impl FlashStringUdf {
    fn new(name: &'static str, field: FlashStringField, params: CombineParams) -> Self {
        let signature = Signature::exact(vec![DataType::Utf8; 6], Volatility::Immutable);
        Self {
            name,
            field,
            params: Arc::new(params),
            signature,
        }
    }
}

#[cfg(feature = "datafusion")]
impl ScalarUDFImpl for FlashStringUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        if arrays.len() != 6 {
            return Err(DataFusionError::Internal(
                format!(
                    "{} expects 6 arguments, received {}",
                    self.name,
                    arrays.len()
                )
                .into(),
            ));
        }

        let tag1 = as_string_array(&arrays[0])?;
        let seq1 = as_string_array(&arrays[1])?;
        let qual1 = as_string_array(&arrays[2])?;
        let tag2 = as_string_array(&arrays[3])?;
        let seq2 = as_string_array(&arrays[4])?;
        let qual2 = as_string_array(&arrays[5])?;

        let len = tag1.len();
        let mut builder = StringBuilder::new();

        for i in 0..len {
            if tag1.is_null(i)
                || seq1.is_null(i)
                || qual1.is_null(i)
                || tag2.is_null(i)
                || seq2.is_null(i)
                || qual2.is_null(i)
            {
                builder.append_null();
                continue;
            }

            let outcome = combine_pair_from_strs(
                tag1.value(i),
                seq1.value(i),
                qual1.value(i),
                tag2.value(i),
                seq2.value(i),
                qual2.value(i),
                self.params.as_ref(),
            )
            .map_err(to_df_error)?;

            let value = match self.field {
                FlashStringField::Tag => outcome.combined_tag,
                FlashStringField::Seq => outcome.combined_seq,
                FlashStringField::Qual => outcome.combined_qual,
            };

            if let Some(v) = value {
                builder.append_value(&v);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(feature = "datafusion")]
#[derive(Debug)]
struct FlashBoolUdf {
    params: Arc<CombineParams>,
    signature: Signature,
}

#[cfg(feature = "datafusion")]
impl FlashBoolUdf {
    fn new(params: CombineParams) -> Self {
        let signature = Signature::exact(vec![DataType::Utf8; 6], Volatility::Immutable);
        Self {
            params: Arc::new(params),
            signature,
        }
    }
}

#[cfg(feature = "datafusion")]
impl ScalarUDFImpl for FlashBoolUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        FLASH_IS_COMBINED_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        if arrays.len() != 6 {
            return Err(DataFusionError::Internal(
                format!(
                    "{} expects 6 arguments, received {}",
                    FLASH_IS_COMBINED_UDF,
                    arrays.len()
                )
                .into(),
            ));
        }

        let tag1 = as_string_array(&arrays[0])?;
        let seq1 = as_string_array(&arrays[1])?;
        let qual1 = as_string_array(&arrays[2])?;
        let tag2 = as_string_array(&arrays[3])?;
        let seq2 = as_string_array(&arrays[4])?;
        let qual2 = as_string_array(&arrays[5])?;

        let len = tag1.len();
        let mut builder = BooleanBuilder::new();

        for i in 0..len {
            if tag1.is_null(i)
                || seq1.is_null(i)
                || qual1.is_null(i)
                || tag2.is_null(i)
                || seq2.is_null(i)
                || qual2.is_null(i)
            {
                builder.append_value(false);
                continue;
            }

            let outcome = combine_pair_from_strs(
                tag1.value(i),
                seq1.value(i),
                qual1.value(i),
                tag2.value(i),
                seq2.value(i),
                qual2.value(i),
                self.params.as_ref(),
            )
            .map_err(to_df_error)?;

            builder.append_value(outcome.is_combined);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(feature = "datafusion")]
#[derive(Debug, Clone)]
pub struct FlashPlans {
    pub combined: LogicalPlan,
    pub not_combined_left: LogicalPlan,
    pub not_combined_right: LogicalPlan,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_config_builder_roundtrips() {
        let cfg = FlashJobConfig::new("a", "b", "c", "prefix");
        let job = FlashDistributedJob::new(cfg.clone(), CombineParams::default());
        assert_eq!(job.config().forward, cfg.forward);
        assert_eq!(job.config().output_prefix, cfg.output_prefix);
    }
}

#[cfg(all(test, feature = "datafusion"))]
mod df_plan_tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn flash_plans_align_with_reference_outputs() -> anyhow::Result<()> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let data_dir = manifest_dir.join("../../FLASH-lowercase-overhang");

        let forward = data_dir.join("input1.fq");
        let reverse = data_dir.join("input2.fq");

        if !forward.exists() || !reverse.exists() {
            // Skip the regression check when reference inputs are unavailable.
            return Ok(());
        }

        let job = FlashDistributedJob::new(
            FlashJobConfig::new(&forward, &reverse, std::env::temp_dir(), "df_test"),
            CombineParams::default(),
        );

        let ctx = job.session_context().await?;
        job.register_fastq_sources(&ctx).await?;
        let plans = job.build_flash_plans(&ctx).await?;

        let combined_batches = ctx
            .execute_logical_plan(plans.combined.clone())
            .await?
            .collect()
            .await?;

        assert!(!combined_batches.is_empty());
        let combined_tags = combined_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(combined_tags.value(0), "@ERR188245.23");

        let not_left_batches = ctx
            .execute_logical_plan(plans.not_combined_left.clone())
            .await?
            .collect()
            .await?;
        let left_tags = not_left_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(left_tags.value(0), "@ERR188245.1");

        Ok(())
    }

    #[tokio::test]
    async fn datafusion_outputs_match_cli_results() -> anyhow::Result<()> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let forward = manifest_dir.join("../input1.fq");
        let reverse = manifest_dir.join("../input2.fq");
        let tmp = tempdir()?;

        let params = CombineParams::default();
        merge_fastq_files(&forward, &reverse, tmp.path(), "ref", &params)?;

        let df_job = FlashDistributedJob::new(
            FlashJobConfig::new(&forward, &reverse, tmp.path(), "df"),
            params,
        );
        df_job.execute_datafusion().await?;

        for suffix in [
            "extendedFrags.fastq",
            "notCombined_1.fastq",
            "notCombined_2.fastq",
        ] {
            let expected = fs::read(tmp.path().join(format!("ref.{suffix}")))?;
            let actual = fs::read(tmp.path().join(format!("df.{suffix}")))?;
            assert_eq!(actual, expected, "mismatch for {suffix}");
        }

        Ok(())
    }
}
