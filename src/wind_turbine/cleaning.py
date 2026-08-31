from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def clean_turbine_data(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separate valid turbine measurements from invalid records.
    """

    validated_df = df.withColumn(
        "rejection_reason",
        when(col("timestamp").isNull(), "missing_timestamp")
        .when(col("turbine_id").isNull(), "missing_turbine_id")
        .when(col("wind_speed").isNull(), "missing_wind_speed")
        .when(col("wind_speed") < 0, "invalid_wind_speed")
        .when(col("wind_direction").isNull(), "missing_wind_direction")
        .when(
            ~col("wind_direction").between(0, 359),
            "invalid_wind_direction",
        )
        .when(col("power_output").isNull(), "missing_power_output")
        .when(col("power_output") < 0, "invalid_power_output")
    )

    rejected_df = validated_df.filter(
        col("rejection_reason").isNotNull()
    )

    cleaned_df = (
        validated_df
        .filter(col("rejection_reason").isNull())
        .drop("rejection_reason")
        .dropDuplicates(["timestamp", "turbine_id"])
    )

    return cleaned_df, rejected_df