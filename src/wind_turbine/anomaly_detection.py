from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, stddev, abs
from pyspark.sql.window import Window


def detect_anomalies(df: DataFrame) -> DataFrame:
    """
    Identify turbine measurements more than two standard deviations
    from the fleet mean at the same timestamp.
    """

    fleet_window = Window.partitionBy("timestamp")

    return (
        df
        .withColumn(
            "fleet_avg_power",
            avg("power_output").over(fleet_window),
        )
        .withColumn(
            "fleet_stddev_power",
            stddev("power_output").over(fleet_window),
        )
        .filter(
            abs(col("power_output") - col("fleet_avg_power"))
            > (2 * col("fleet_stddev_power"))
        )
    )