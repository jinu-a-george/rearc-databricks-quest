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
                )
            ),
            True,
        ),
    ]
)
LATEST_SCHEMA = StructType(
    [
        StructField("snapshot_id", StringType(), True),
        StructField("status", StringType(), True),
    ]
)
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


def _read_bls_text(filename: str) -> DataFrame:
    dataframe = spark.read.text(f"{BLS_PATH}/snapshots/*/{filename}")
    return _with_snapshot_metadata(dataframe, "BLS", filename)


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
    required_column: str,
    corrupt_column: str | None = None,
) -> DataFrame:
    malformed = (
        F.col(corrupt_column).isNotNull()
        if corrupt_column
        else F.lit(False)
    )
    missing_key = F.col(required_column).isNull() | (
        F.trim(F.col(required_column)) == ""
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


@dp.materialized_view(name="pr_class", comment="Raw BLS class lookup.")
def pr_class():
    return _read_bls_tsv("pr.class")


@dp.materialized_view(name="pr_data_0_current", comment="Raw BLS current observations.")
def pr_data_0_current():
    return _read_bls_tsv("pr.data.0.Current")


@dp.materialized_view(name="pr_data_1_alldata", comment="Raw historical BLS observations.")
def pr_data_1_alldata():
    return _read_bls_tsv("pr.data.1.AllData")


@dp.materialized_view(name="pr_contacts", comment="Raw BLS contacts document lines.")
def pr_contacts():
    return _read_bls_text("pr.contacts")


@dp.materialized_view(name="pr_duration", comment="Raw BLS duration lookup.")
def pr_duration():
    return _read_bls_tsv("pr.duration")


@dp.materialized_view(name="pr_footnote", comment="Raw BLS footnote lookup.")
def pr_footnote():
    return _read_bls_tsv("pr.footnote")


@dp.materialized_view(name="pr_measure", comment="Raw BLS measure lookup.")
def pr_measure():
    return _read_bls_tsv("pr.measure")


@dp.materialized_view(name="pr_period", comment="Raw BLS period lookup.")
def pr_period():
    return _read_bls_tsv("pr.period")


@dp.materialized_view(name="pr_seasonal", comment="Raw BLS seasonal adjustment lookup.")
def pr_seasonal():
    return _read_bls_tsv("pr.seasonal")


@dp.materialized_view(name="pr_sector", comment="Raw BLS sector lookup.")
def pr_sector():
    return _read_bls_tsv("pr.sector")


@dp.materialized_view(name="pr_series", comment="Raw BLS series metadata.")
def pr_series():
    return _read_bls_tsv("pr.series")


@dp.materialized_view(name="pr_txt", comment="Raw BLS documentation lines.")
def pr_txt():
    return _read_bls_text("pr.txt")


@dp.materialized_view(
    name="population",
    comment="Raw DataUSA population records with source fields stored as strings.",
)
def population():
    return _read_population()


@dp.materialized_view(
    name="quarantine",
    comment="Malformed Bronze records retained with snapshot traceability.",
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
                key_column,
                "_corrupt_record",
            )
        )

    quarantine_frames.append(
        _quarantine_rows(
            _read_population(),
            "DataUSA",
            "population.json",
            ["nation_id", "nation", "year", "population"],
            "nation_id",
        )
    )

    result = quarantine_frames[0]
    for dataframe in quarantine_frames[1:]:
        result = result.unionByName(dataframe)
    return result
