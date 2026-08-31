from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, round, min, max, avg, count


def calculate_daily_summary(df: DataFrame) -> DataFrame:
    """
    Calculate daily power output statistics for each turbine.
    """

    return (
        df
        # Use calendar day as the 24-hour reporting period
        .withColumn("date", to_date("timestamp"))
        .groupBy("date", "turbine_id")
        .agg(
            round(
                min("power_output"), 2
            ).alias("min_power_output"),

            round(
                max("power_output"), 2
            ).alias("max_power_output"),

            round(
                avg("power_output"), 2
            ).alias("avg_power_output"),

            # Hourly data should normally contain 24 measurements per day
            count("power_output").alias("measurement_count"),
        )
    )