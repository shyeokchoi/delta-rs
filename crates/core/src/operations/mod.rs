//! High level operations API to interact with Delta tables
//!
//! At the heart of the high level operations APIs is the [`DeltaOps`] struct,
//! which consumes a [`DeltaTable`] and exposes methods to attain builders for
//! several high level operations. The specific builder structs allow fine-tuning
//! the operations' behaviors and will return an updated table potentially in conjunction
//! with a [data stream][datafusion::physical_plan::SendableRecordBatchStream],
//! if the operation returns data as well.
use rand::Rng;
use std::cell::Cell;
use std::collections::HashMap;
use std::future::IntoFuture;
use tokio::task_local;

use add_feature::AddTableFeatureBuilder;
#[cfg(feature = "datafusion")]
use arrow_array::RecordBatch;
#[cfg(feature = "datafusion")]
pub use datafusion_physical_plan::common::collect as collect_sendable_stream;

use self::add_column::AddColumnBuilder;
use self::create::CreateBuilder;
use self::filesystem_check::FileSystemCheckBuilder;
use self::optimize::OptimizeBuilder;
use self::restore::RestoreBuilder;
use self::set_tbl_properties::SetTablePropertiesBuilder;
use self::vacuum::VacuumBuilder;
#[cfg(feature = "datafusion")]
use self::{
    constraints::ConstraintBuilder, datafusion_utils::Expression, delete::DeleteBuilder,
    drop_constraints::DropConstraintBuilder, load::LoadBuilder, load_cdf::CdfLoadBuilder,
    merge::MergeBuilder, update::UpdateBuilder, write::WriteBuilder,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::open_table;
use crate::protocol::SaveMode;
use crate::table::builder::DeltaTableBuilder;
use crate::DeltaTable;

pub mod add_column;
pub mod add_feature;
pub mod cast;
pub mod convert_to_delta;
pub mod create;
pub mod drop_constraints;
pub mod filesystem_check;
pub mod optimize;
pub mod restore;
pub mod transaction;
pub mod vacuum;

#[cfg(all(feature = "cdf", feature = "datafusion"))]
mod cdc;
#[cfg(feature = "datafusion")]
pub mod constraints;
#[cfg(feature = "datafusion")]
pub mod delete;
#[cfg(feature = "datafusion")]
mod load;
#[cfg(feature = "datafusion")]
pub mod load_cdf;
#[cfg(feature = "datafusion")]
pub mod merge;
pub mod set_tbl_properties;
#[cfg(feature = "datafusion")]
pub mod update;
#[cfg(feature = "datafusion")]
pub mod write;
pub mod writer;

#[allow(unused)]
/// The [Operation] trait defines common behaviors that all operations builders
/// should have consistent
pub(crate) trait Operation<State>: std::future::IntoFuture {}

/// High level interface for executing commands against a DeltaTable
pub struct DeltaOps(pub DeltaTable);

static DEFAULT_SCALING_FACTOR: u64 = 100;
static SECOND_IN_MILLIS: u64 = 1000;

/// Configuration for random exponential backoff
#[derive(Clone)]
pub struct RetryConfig {
    /// Scaling factor for random retry interval
    /// It is multipled to (backoff_factor)^(retry count) to get the max interval
    /// default: DEFAULT_SCALING_FACTOR
    pub scaling_factor: u64,
    /// backoff factor which is multiplied to the interval, every time a retry is performed
    pub backoff_factor: u64,
}

impl RetryConfig {
    /// Create a new RetryConfig
    pub fn new(scaling_factor: u64, backoff_factor: u64) -> Self {
        RetryConfig {
            scaling_factor,
            backoff_factor,
        }
    }

    /// default RetryConfig
    pub fn default() -> Self {
        RetryConfig {
            scaling_factor: DEFAULT_SCALING_FACTOR,
            backoff_factor: 2,
        }
    }
}

/// Retry mode for write conflicts
#[derive(Clone)]
pub enum RetryMode {
    /// Retry immediately
    Immediate,
    /// Retry with random, exponential backoff
    RandomExponential(RetryConfig),
}

/// Types of cloud storage access
#[derive(Hash, Eq, PartialEq)]
pub enum CloudStorageAccessType {
    /// Cloud Storage object list API
    ListObjects,
    /// Cloud Storage download object API
    ReadObject,
}

/// Result of a commit
pub enum CommitResult {
    /// Success with metrics
    Success(SucceessCommitMetrics),
    /// Failure with metrics
    Fail(FailedCommitMetrics),
}

/// Represents the metrics of a successful commit
pub struct SucceessCommitMetrics {
    /// The number of retries made during the commit
    pub retry_cnt: u32,
    /// The time taken to commit
    pub commit_duration: std::time::Duration,
    /// The number of cloud storage accesses made during the commit
    pub cloud_storage_accesses: CloudStorageAccessCountMap,
}

impl SucceessCommitMetrics {
    /// Create a new SucceessCommitMetrics
    pub fn new(
        retry_cnt: u32,
        commit_duration: std::time::Duration,
        cloud_storage_accesses: CloudStorageAccessCountMap,
    ) -> Self {
        SucceessCommitMetrics {
            retry_cnt,
            commit_duration,
            cloud_storage_accesses,
        }
    }
}

/// Represents the metrics of a failed commit
pub struct FailedCommitMetrics {
    /// The time taken
    pub commit_duration: std::time::Duration,
    /// The number of cloud storage accesses made during the trial of commit
    pub cloud_storage_accesses: CloudStorageAccessCountMap,
}

impl FailedCommitMetrics {
    /// Create a new FailedCommitMetrics
    pub fn new(
        commit_duration: std::time::Duration,
        cloud_storage_accesses: CloudStorageAccessCountMap,
    ) -> Self {
        FailedCommitMetrics {
            commit_duration,
            cloud_storage_accesses,
        }
    }
}

/// Calculate the interval for random exponential backoff
/// Unit: millis
fn get_random_exponential_backoff_interval_in_millis(
    scaling_factor: u64,
    base: u64,     /* the base of exponential backoff */
    exponent: u32, /* the exponent of exponential backoff */
) -> u64 {
    let max_backoff = scaling_factor * base.pow(exponent);
    let capped_backoff = std::cmp::min(max_backoff, SECOND_IN_MILLIS);
    return rand::thread_rng().gen_range(0..capped_backoff);
}

/// A map to deliver the result of Cloud Storage access count
pub struct CloudStorageAccessCountMap {
    inner: HashMap<CloudStorageAccessType, usize>,
}

impl CloudStorageAccessCountMap {
    /// Create a new CloudStorageAccessCountMap
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    fn finalize(mut self) -> Self {
        self.increment(
            CloudStorageAccessType::ListObjects,
            LIST_OBJECTS.with(|cnt| cnt.get()),
        );
        self.increment(
            CloudStorageAccessType::ReadObject,
            READ_OBJECT.with(|cnt| cnt.get()),
        );
        self
    }

    fn increment(&mut self, access_type: CloudStorageAccessType, increment: usize) {
        let count = self.inner.entry(access_type).or_insert(0);
        *count += increment
    }

    /// get the count of the access type
    fn get(&self, access_type: CloudStorageAccessType) -> usize {
        *self.inner.get(&access_type).unwrap_or(&0)
    }

    /// get the count of the ListObjects API access
    pub fn get_list_objects_count(&self) -> usize {
        self.get(CloudStorageAccessType::ListObjects)
    }

    /// get the count of the ReadObject API access
    pub fn get_read_object_count(&self) -> usize {
        self.get(CloudStorageAccessType::ReadObject)
    }
}

task_local! {
    /// Count the number of Cloud Storage List Objects API access
pub static LIST_OBJECTS: Cell<usize>;
    /// Count the number of Cloud Storage Download Object API access
pub static READ_OBJECT: Cell<usize>;
}

/// attempt to write with retry (immediate OR random exponential backoff)
pub async fn attempt_write_with_retry(
    table_url: &str,
    batches: impl IntoIterator<Item = RecordBatch> + Clone,
    save_mode: SaveMode,
    retry_mode: RetryMode,
    max_retry: u32,
) -> DeltaResult<CommitResult> {
    LIST_OBJECTS.with(|cnt| cnt.set(0));
    READ_OBJECT.with(|cnt| cnt.set(0));

    let start = std::time::Instant::now();

    for retry_cnt in 0..=max_retry {
        // open table stored in the Cloud Storage
        // this will read the `_delta_log` directory and construct the state of the table
        let table = open_table(table_url).await?;

        // try writing
        let write_res = DeltaOps(table)
            .write(batches.clone())
            .with_target_file_size(1000)
            .with_save_mode(save_mode)
            .into_future()
            .await;

        // match the write result
        // retry if necessary
        match write_res {
            Ok(_) => {
                // success
                return Ok(CommitResult::Success(SucceessCommitMetrics::new(
                    retry_cnt,
                    start.elapsed(),
                    CloudStorageAccessCountMap::new().finalize(),
                )));
            }
            Err(DeltaTableError::VersionAlreadyExists(_)) => {
                // write conflict!
                // random exponential backoff or immediate retry (based on retry_mode)
                if let RetryMode::RandomExponential(config) = &retry_mode {
                    if retry_cnt < max_retry {
                        let interval = get_random_exponential_backoff_interval_in_millis(
                            config.scaling_factor,
                            config.backoff_factor,
                            retry_cnt,
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(interval)).await;
                    }
                }
            }
            Err(err) => {
                // error
                return Err(err);
            }
        }
    }

    Ok(CommitResult::Fail(FailedCommitMetrics::new(
        start.elapsed(),
        CloudStorageAccessCountMap::new().finalize(),
    )))
}

impl DeltaOps {
    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given uri.
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    /// };
    /// ```
    pub async fn try_from_uri(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri).build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// try from uri with storage options
    pub async fn try_from_uri_with_storage_options(
        uri: impl AsRef<str>,
        storage_options: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri)
            .with_storage_options(storage_options)
            .build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// Create a new [`DeltaOps`] instance, backed by an un-initialized in memory table
    ///
    /// Using this will not persist any changes beyond the lifetime of the table object.
    /// The main purpose of in-memory tables is for use in testing.
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    ///
    /// let ops = DeltaOps::new_in_memory();
    /// ```
    #[must_use]
    pub fn new_in_memory() -> Self {
        DeltaTableBuilder::from_uri("memory://")
            .build()
            .unwrap()
            .into()
    }

    /// Create a new Delta table
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    ///     let table = ops.create().with_table_name("my_table").await.unwrap();
    ///     assert_eq!(table.version(), 0);
    /// };
    /// ```
    #[must_use]
    pub fn create(self) -> CreateBuilder {
        CreateBuilder::default().with_log_store(self.0.log_store)
    }

    /// Load data from a DeltaTable
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn load(self) -> LoadBuilder {
        LoadBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Load a table with CDF Enabled
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn load_cdf(self) -> CdfLoadBuilder {
        CdfLoadBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn write(self, batches: impl IntoIterator<Item = RecordBatch>) -> WriteBuilder {
        WriteBuilder::new(self.0.log_store, self.0.state).with_input_batches(batches)
    }

    /// Vacuum stale files from delta table
    #[must_use]
    pub fn vacuum(self) -> VacuumBuilder {
        VacuumBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn filesystem_check(self) -> FileSystemCheckBuilder {
        FileSystemCheckBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn optimize<'a>(self) -> OptimizeBuilder<'a> {
        OptimizeBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Delete data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn delete(self) -> DeleteBuilder {
        DeleteBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn update(self) -> UpdateBuilder {
        UpdateBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Restore delta table to a specified version or datetime
    #[must_use]
    pub fn restore(self) -> RestoreBuilder {
        RestoreBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn merge<E: Into<Expression>>(
        self,
        source: datafusion::prelude::DataFrame,
        predicate: E,
    ) -> MergeBuilder {
        MergeBuilder::new(
            self.0.log_store,
            self.0.state.unwrap(),
            predicate.into(),
            source,
        )
    }

    /// Add a check constraint to a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn add_constraint(self) -> ConstraintBuilder {
        ConstraintBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Enable a table feature for a table
    #[must_use]
    pub fn add_feature(self) -> AddTableFeatureBuilder {
        AddTableFeatureBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Drops constraints from a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn drop_constraints(self) -> DropConstraintBuilder {
        DropConstraintBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Set table properties
    pub fn set_tbl_properties(self) -> SetTablePropertiesBuilder {
        SetTablePropertiesBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Add new columns
    pub fn add_columns(self) -> AddColumnBuilder {
        AddColumnBuilder::new(self.0.log_store, self.0.state.unwrap())
    }
}

impl From<DeltaTable> for DeltaOps {
    fn from(table: DeltaTable) -> Self {
        Self(table)
    }
}

impl From<DeltaOps> for DeltaTable {
    fn from(ops: DeltaOps) -> Self {
        ops.0
    }
}

impl AsRef<DeltaTable> for DeltaOps {
    fn as_ref(&self) -> &DeltaTable {
        &self.0
    }
}

/// Get the num_idx_columns and stats_columns from the table configuration in the state
/// If table_config does not exist (only can occur in the first write action) it takes
/// the configuration that was passed to the writerBuilder.
pub fn get_num_idx_cols_and_stats_columns(
    config: Option<crate::table::config::TableConfig<'_>>,
    configuration: HashMap<String, Option<String>>,
) -> (i32, Option<Vec<String>>) {
    let (num_index_cols, stats_columns) = match &config {
        Some(conf) => (conf.num_indexed_cols(), conf.stats_columns()),
        _ => (
            configuration
                .get("delta.dataSkippingNumIndexedCols")
                .and_then(|v| v.clone().map(|v| v.parse::<i32>().unwrap()))
                .unwrap_or(crate::table::config::DEFAULT_NUM_INDEX_COLS),
            configuration
                .get("delta.dataSkippingStatsColumns")
                .and_then(|v| v.as_ref().map(|v| v.split(',').collect::<Vec<&str>>())),
        ),
    };
    (
        num_index_cols,
        stats_columns
            .clone()
            .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
    )
}

/// Get the target_file_size from the table configuration in the sates
/// If table_config does not exist (only can occur in the first write action) it takes
/// the configuration that was passed to the writerBuilder.
pub(crate) fn get_target_file_size(
    config: &Option<crate::table::config::TableConfig<'_>>,
    configuration: &HashMap<String, Option<String>>,
) -> i64 {
    match &config {
        Some(conf) => conf.target_file_size(),
        _ => configuration
            .get("delta.targetFileSize")
            .and_then(|v| v.clone().map(|v| v.parse::<i64>().unwrap()))
            .unwrap_or(crate::table::config::DEFAULT_TARGET_FILE_SIZE),
    }
}

#[cfg(feature = "datafusion")]
mod datafusion_utils {
    use datafusion::execution::context::SessionState;
    use datafusion_common::DFSchema;
    use datafusion_expr::Expr;

    use crate::{delta_datafusion::expr::parse_predicate_expression, DeltaResult};

    /// Used to represent user input of either a Datafusion expression or string expression
    #[derive(Debug)]
    pub enum Expression {
        /// Datafusion Expression
        DataFusion(Expr),
        /// String Expression
        String(String),
    }

    impl From<Expr> for Expression {
        fn from(val: Expr) -> Self {
            Expression::DataFusion(val)
        }
    }

    impl From<&str> for Expression {
        fn from(val: &str) -> Self {
            Expression::String(val.to_string())
        }
    }
    impl From<String> for Expression {
        fn from(val: String) -> Self {
            Expression::String(val)
        }
    }

    pub(crate) fn into_expr(
        expr: Expression,
        schema: &DFSchema,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        match expr {
            Expression::DataFusion(expr) => Ok(expr),
            Expression::String(s) => parse_predicate_expression(schema, s, df_state),
        }
    }

    pub(crate) fn maybe_into_expr(
        expr: Option<Expression>,
        schema: &DFSchema,
        df_state: &SessionState,
    ) -> DeltaResult<Option<Expr>> {
        Ok(match expr {
            Some(predicate) => Some(into_expr(predicate, schema, df_state)?),
            None => None,
        })
    }
}
