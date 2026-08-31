from datetime import datetime

from wind_turbine.transformations import calculate_daily_summary


def test_calculate_daily_summary(spark):
    # Three measurements for the same turbine and calendar day
    data = [
        (datetime(2022, 3, 1, 0, 0), 1, 2.0),
        (datetime(2022, 3, 1, 1, 0), 1, 3.0),
        (datetime(2022, 3, 1, 2, 0), 1, 4.0),
    ]

    df = spark.createDataFrame(
        data,
        ["timestamp", "turbine_id", "power_output"],
    )

    result = calculate_daily_summary(df).collect()[0]

    assert result["min_power_output"] == 2.0
    assert result["max_power_output"] == 4.0
    assert result["avg_power_output"] == 3.0
    assert result["measurement_count"] == 3