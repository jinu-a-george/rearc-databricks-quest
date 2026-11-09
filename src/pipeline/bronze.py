from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)


RAW_VOLUME_PATH = spark.conf.get(
    "raw_volume_path", "/Volumes/rearc_catalog/data_quest_bronze/raw_data"
)
BLS_PATH = f"{RAW_VOLUME_PATH}/bls"
POPULATION_PATH = f"{RAW_VOLUME_PATH}/population"
MANIFEST_PATH = f"{RAW_VOLUME_PATH}/manifests/*.json"

BLS_SCHEMAS = {
    "pr.class": (
        ["class_code", "class_text", "display_level", "selectable", "sort_sequence"],
        "class_code",
    ),
    "pr.data.0.Current": (
        ["series_id", "year", "period", "value", "footnote_codes"],
        "series_id",
    ),
    "pr.data.1.AllData": (
        ["series_id", "year", "period", "value", "footnote_codes"],
        "series_id",
    ),
    "pr.duration": (
        ["duration_code", "duration_text", "display_level", "selectable", "sort_sequence"],
        "duration_code",
    ),
    "pr.footnote": (["footnote_code", "footnote_text"], "footnote_code"),
    "pr.measure": (
        ["measure_code", "measure_text", "display_level", "selectable", "sort_sequence"],
        "measure_code",
    ),
    "pr.period": (["period", "period_abbr", "period_name"], "period"),
    "pr.seasonal": (["Seasonal_code", "Seasonal_text"], "Seasonal_code"),
    "pr.sector": (
        ["sector_code", "sector_name", "display_level", "selectable", "sort_sequence"],
        "sector_code",
    ),
    "pr.series": (
        [
            "series_id",
            "sector_code",
            "class_code",
            "measure_code",
            "duration_code",
            "seasonal",
            "base_year",
            "footnote_codes",
            "series_title",
            "begin_year",
            "begin_period",
            "end_year",
            "end_period",
        ],
        "series_id",
    ),
}

MANIFEST_SCHEMA = StructType(
    [
        StructField("snapshot_id", StringType(), True),
        StructField("ingestion_status", StringType(), True),
        StructField(
            "files",
            ArrayType(
                StructType(
                    [
                        StructField("source_name", StringType(), True),
                        StructField("source_file", StringType(), True),
                        StructField("checksum_sha256", StringType(), True),
                        StructField("downloaded_at", StringType(), True),
                        StructField("ingestion_status", StringType(), True),
                    ]
                ),
                True,
            ),
        ),
    ]
)
LATEST_SCHEMA = StructType(
    [
        StructField("snapshot_id", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

INGESTION_COLUMN_COMMENTS = {
    "_snapshot_id": "Identifier of the committed coordinated source snapshot.",
    "_ingested_at": "Timestamp the source file was downloaded for this snapshot.",
}

BLS_COLUMN_COMMENTS = {
    "pr.class": {
        "class_code": "Code identifying the employee group represented by a series.",
        "class_text": "Human-readable name of the employee group.",
        "display_level": "Hierarchy level used with sort_sequence in BLS displays.",
        "selectable": "Whether this item can be selected in query tools (T or F).",
        "sort_sequence": "Sort order used with display_level in BLS displays.",
    },
    "pr.data.0.Current": {
        "series_id": "BLS identifier for the specific productivity time series.",
        "year": "Calendar year of the data observation.",
        "period": "Quarter or annual-average period code for the observation.",
        "value": "Source-reported numeric value for the series and period.",
        "footnote_codes": "Comma-separated footnote codes applying to the observation.",
    },
    "pr.data.1.AllData": {
        "series_id": "BLS identifier for the specific productivity time series.",
        "year": "Calendar year of the data observation.",
        "period": "Quarter or annual-average period code for the observation.",
        "value": "Source-reported numeric value for the series and period.",
        "footnote_codes": "Comma-separated footnote codes applying to the observation.",
    },
    "pr.duration": {
        "duration_code": "Code identifying whether a measure is a percent change or index.",
        "duration_text": "Description of the measure's duration or comparison basis.",
        "display_level": "Hierarchy level used with sort_sequence in BLS displays.",
        "selectable": "Whether this item can be selected in query tools (T or F).",
        "sort_sequence": "Sort order used with display_level in BLS displays.",
    },
    "pr.footnote": {
        "footnote_code": "Code identifying an individual BLS footnote.",
        "footnote_text": "Text describing the footnote.",
    },
    "pr.measure": {
        "measure_code": "Code identifying the specific factor being measured.",
        "measure_text": "Human-readable name of the measured factor.",
        "display_level": "Hierarchy level used with sort_sequence in BLS displays.",
        "selectable": "Whether this item can be selected in query tools (T or F).",
        "sort_sequence": "Sort order used with display_level in BLS displays.",
    },
    "pr.period": {
        "period": "Code for the observation period; Q05 represents the annual average.",
        "period_abbr": "Abbreviated label for the observation period.",
        "period_name": "Human-readable name of the observation period.",
    },
    "pr.seasonal": {
        "Seasonal_code": "Code indicating seasonal-adjustment status (S or U).",
        "Seasonal_text": "Description of the seasonal-adjustment status.",
    },
    "pr.sector": {
        "sector_code": "Code identifying the economic sector.",
        "sector_name": "Human-readable name of the economic sector.",
        "display_level": "Hierarchy level used with sort_sequence in BLS displays.",
        "selectable": "Whether this item can be selected in query tools (T or F).",
        "sort_sequence": "Sort order used with display_level in BLS displays.",
    },
    "pr.series": {
        "series_id": "BLS identifier composed from survey and series dimension codes.",
        "sector_code": "Code identifying the economy sector for the series.",
        "class_code": "Code identifying the employee group for the series.",
        "measure_code": "Code identifying the factor measured by the series.",
        "duration_code": "Code identifying the measure's duration or comparison basis.",
        "seasonal": "Code indicating whether the series is seasonally adjusted.",
        "base_year": "Base year for the index presented, when applicable.",
        "footnote_codes": "Comma-separated footnote codes applying to the series.",
        "series_title": "Source-provided human-readable title for the series.",
        "begin_year": "First year for which observations are available.",
        "begin_period": "First observation period within begin_year.",
        "end_year": "Last year for which observations are available.",
        "end_period": "Last observation period within end_year.",
    },
}

POPULATION_COLUMN_COMMENTS = {
    "nation_id": "DataUSA identifier for the nation.",
    "nation": "DataUSA human-readable nation name.",
    "year": "Calendar year associated with the population estimate.",
    "population": "Source-reported population count, retained as text in Bronze.",
}

QUARANTINE_COLUMN_COMMENTS = {
    "source_name": "Source system from which the rejected record originated.",
    "source_file": "Source filename associated with the rejected record.",
    "raw_record": "Original record text or serialized source fields.",
    "rejection_reason": "Reason the record was routed to quarantine.",
    "rejected_at": "Timestamp when the quarantine row was generated.",
    **INGESTION_COLUMN_COMMENTS,
}


def _schema_with_comments(
    fields: list[tuple[str, str]], comments: dict[str, str]
) -> str:
    field_names = {name for name, _ in fields}
    missing_comments = field_names - comments.keys()
    if missing_comments:
        raise ValueError(
            f"Missing column comments for: {', '.join(sorted(missing_comments))}"
        )
    schema_fields = []
    for name, data_type in fields:
        comment = comments[name].replace("'", "''")
        schema_fields.append(f"`{name}` {data_type} COMMENT '{comment}'")
    return ",\n".join(schema_fields)


def _bls_table_schema(filename: str) -> str:
    source_fields = BLS_SCHEMAS[filename][0]
    fields = [(name, "STRING") for name in source_fields]
    fields.extend(
        [("_snapshot_id", "STRING"), ("_ingested_at", "TIMESTAMP")]
    )
    comments = {
        **BLS_COLUMN_COMMENTS[filename],
        **INGESTION_COLUMN_COMMENTS,
    }
    return _schema_with_comments(fields, comments)


def _successful_file_index(
    source_name: str, source_file: str | None = None
) -> DataFrame:
    latest_snapshot = (
        spark.read.schema(LATEST_SCHEMA)
        .json(f"{RAW_VOLUME_PATH}/latest.json")
        .filter(F.col("status") == "SUCCESS")
        .select(F.col("snapshot_id").alias("_committed_snapshot_id"))
    )
    manifests = (
        spark.read.schema(MANIFEST_SCHEMA)
        .json(MANIFEST_PATH)
        .crossJoin(latest_snapshot)
        .filter(F.col("snapshot_id") <= F.col("_committed_snapshot_id"))
    )
    entries = (
        manifests.filter(F.col("ingestion_status") == "SUCCESS")
        .select("snapshot_id", F.explode("files").alias("file"))
        .filter(
            (F.col("file.source_name") == source_name)
            & (F.col("file.ingestion_status") == "SUCCESS")
        )
    )
    if source_file is not None:
        entries = entries.filter(F.col("file.source_file") == source_file)
    return entries.select(
        F.col("snapshot_id").alias("_snapshot_id"),
        F.to_timestamp("file.downloaded_at").alias("_ingested_at"),
    )


def _with_snapshot_metadata(
    dataframe: DataFrame,
    source_name: str,
    source_file: str | None = None,
) -> DataFrame:
    source_columns = [
        column for column in dataframe.columns if column != "_source_file_path"
    ]
    if "_source_file_path" not in dataframe.columns:
        dataframe = dataframe.withColumn(
            "_source_file_path", F.col("_metadata.file_path")
        )
    dataframe = dataframe.withColumn(
        "_snapshot_id",
        F.regexp_extract(
            F.col("_source_file_path"),
            r"/snapshots/([0-9]{20})/",
            1,
        ),
    )
    file_index = _successful_file_index(source_name, source_file)
    return (
        dataframe.join(file_index, on="_snapshot_id", how="inner")
        .select(*source_columns, "_snapshot_id", "_ingested_at")
    )


def _read_bls_tsv(
    filename: str, *, include_corrupt_record: bool = False
) -> DataFrame:
    fields, _ = BLS_SCHEMAS[filename]
    schema_fields = [StructField(field, StringType(), True) for field in fields]
    schema_fields.append(StructField("_corrupt_record", StringType(), True))
    dataframe = (
        spark.read.schema(StructType(schema_fields))
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(f"{BLS_PATH}/snapshots/*/{filename}")
    )
    dataframe = _with_snapshot_metadata(dataframe, "BLS", filename)
    if not include_corrupt_record:
        dataframe = dataframe.drop("_corrupt_record")
    return dataframe


def _read_population() -> DataFrame:
    record_schema = StructType(
        [
            StructField("Nation ID", StringType(), True),
            StructField("Nation", StringType(), True),
            StructField("Year", StringType(), True),
            StructField("Population", StringType(), True),
        ]
    )
    response_schema = StructType(
        [
            StructField(
                "data",
                ArrayType(record_schema, containsNull=True),
                True,
            )
        ]
    )
    raw_files = (
        spark.read.option("wholetext", "true")
        .text(f"{POPULATION_PATH}/snapshots/*/population.json")
        .select(
            F.col("value").alias("json_text"),
            F.col("_metadata.file_path").alias("_source_file_path"),
        )
    )
    population_token_pattern = (
        r'("Population"\s*:\s*)(-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)'
    )
    response = raw_files.select(
        F.from_json(
            F.regexp_replace(
                "json_text", population_token_pattern, r'$1"$2"'
            ),
            response_schema,
        ).alias("payload"),
        "_source_file_path",
    )
    dataframe = response.select(
        F.explode("payload.data").alias("record"),
        "_source_file_path",
    ).select("record.*", "_source_file_path")
    records = dataframe.select(
        F.col("`Nation ID`").alias("nation_id"),
        F.col("Nation").alias("nation"),
        F.col("Year").alias("year"),
        F.col("Population").alias("population"),
        "_source_file_path",
    )
    return _with_snapshot_metadata(records, "DataUSA", "population.json")


def _quarantine_rows(
    dataframe: DataFrame,
    source_name: str,
    source_file: str,
    source_columns: list[str],
    required_column: str | None,
    corrupt_column: str | None = None,
) -> DataFrame:
    malformed = (
        F.col(corrupt_column).isNotNull()
        if corrupt_column
        else F.lit(False)
    )
    missing_key = (
        F.col(required_column).isNull() | (F.trim(F.col(required_column)) == "")
        if required_column
        else F.lit(False)
    )
    raw_record = F.coalesce(
        F.col(corrupt_column) if corrupt_column else F.lit(None).cast("string"),
        F.to_json(F.struct(*[F.col(column) for column in source_columns])),
    )
    return (
        dataframe.filter(malformed | missing_key)
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit(source_file).alias("source_file"),
            raw_record.alias("raw_record"),
            F.when(malformed, F.lit("Malformed delimited record"))
            .otherwise(F.lit(f"Missing required identifier: {required_column}"))
            .alias("rejection_reason"),
            F.current_timestamp().alias("rejected_at"),
            "_snapshot_id",
            "_ingested_at",
        )
    )


@dp.materialized_view(
    name="pr_class",
    comment="Source BLS class codes, descriptions, and display-order attributes.",
    schema=_bls_table_schema("pr.class"),
)
def pr_class():
    return _read_bls_tsv("pr.class")


@dp.materialized_view(
    name="pr_data_0_current",
    comment="Source BLS year-to-date productivity observations in TSV form.",
    schema=_bls_table_schema("pr.data.0.Current"),
)
def pr_data_0_current():
    return _read_bls_tsv("pr.data.0.Current")


@dp.materialized_view(
    name="pr_data_1_alldata",
    comment="Source BLS productivity observations across all available years.",
    schema=_bls_table_schema("pr.data.1.AllData"),
)
def pr_data_1_alldata():
    return _read_bls_tsv("pr.data.1.AllData")


@dp.materialized_view(
    name="pr_duration",
    comment="Source BLS duration codes and their descriptions.",
    schema=_bls_table_schema("pr.duration"),
)
def pr_duration():
    return _read_bls_tsv("pr.duration")


@dp.materialized_view(
    name="pr_footnote",
    comment="Source BLS footnote codes and their explanatory text.",
    schema=_bls_table_schema("pr.footnote"),
)
def pr_footnote():
    return _read_bls_tsv("pr.footnote")


@dp.materialized_view(
    name="pr_measure",
    comment="Source BLS measure codes, descriptions, and display-order attributes.",
    schema=_bls_table_schema("pr.measure"),
)
def pr_measure():
    return _read_bls_tsv("pr.measure")


@dp.materialized_view(
    name="pr_period",
    comment="Source BLS observation-period codes and labels.",
    schema=_bls_table_schema("pr.period"),
)
def pr_period():
    return _read_bls_tsv("pr.period")


@dp.materialized_view(
    name="pr_seasonal",
    comment="Source BLS seasonal-adjustment codes and descriptions.",
    schema=_bls_table_schema("pr.seasonal"),
)
def pr_seasonal():
    return _read_bls_tsv("pr.seasonal")


@dp.materialized_view(
    name="pr_sector",
    comment="Source BLS economic-sector codes and descriptions.",
    schema=_bls_table_schema("pr.sector"),
)
def pr_sector():
    return _read_bls_tsv("pr.sector")


@dp.materialized_view(
    name="pr_series",
    comment="Source BLS time-series definitions, dimensions, and observed date bounds.",
    schema=_bls_table_schema("pr.series"),
)
def pr_series():
    return _read_bls_tsv("pr.series")


@dp.materialized_view(
    name="population",
    comment="Source DataUSA population records with source values retained as strings.",
    schema=_schema_with_comments(
        [
            ("nation_id", "STRING"),
            ("nation", "STRING"),
            ("year", "STRING"),
            ("population", "STRING"),
            ("_snapshot_id", "STRING"),
            ("_ingested_at", "TIMESTAMP"),
        ],
        {**POPULATION_COLUMN_COMMENTS, **INGESTION_COLUMN_COMMENTS},
    ),
)
def population():
    return _read_population()


@dp.materialized_view(
    name="quarantine",
    comment="Malformed or key-incomplete source records retained with rejection reasons.",
    schema=_schema_with_comments(
        [
            ("source_name", "STRING"),
            ("source_file", "STRING"),
            ("raw_record", "STRING"),
            ("rejection_reason", "STRING"),
            ("rejected_at", "TIMESTAMP"),
            ("_snapshot_id", "STRING"),
            ("_ingested_at", "TIMESTAMP"),
        ],
        QUARANTINE_COLUMN_COMMENTS,
    ),
)
def quarantine():
    quarantine_frames = []
    for filename, (columns, key_column) in BLS_SCHEMAS.items():
        quarantine_frames.append(
            _quarantine_rows(
                _read_bls_tsv(filename, include_corrupt_record=True),
                "BLS",
                filename,
                columns,
                None if key_column == "series_id" else key_column,
                "_corrupt_record",
            )
        )

    quarantine_frames.append(
        _quarantine_rows(
            _read_population(),
            "DataUSA",
            "population.json",
            ["nation_id", "nation", "year", "population"],
            None,
        )
    )

    result = quarantine_frames[0]
    for dataframe in quarantine_frames[1:]:
        result = result.unionByName(dataframe)
    return result
