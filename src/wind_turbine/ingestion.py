from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)

# Define the expected schema rather than relying on Spark to infer data types
wind_turbine_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("turbine_id", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("power_output", DoubleType(), True),
])

def read_turbine_data(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """
    Read turbine CSV files using the expected schema and add
    basic metadata for data lineage.
    """

    return (
        spark.read
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(wind_turbine_schema)
        .csv(input_path)

        # Track which source file each measurement came from
        .withColumn("source_file", input_file_name())

        # Record when the data was processed
        .withColumn("ingested_at", current_timestamp())
    )