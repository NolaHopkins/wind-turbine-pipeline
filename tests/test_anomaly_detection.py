from datetime import datetime

from wind_turbine.anomaly_detection import detect_anomalies


def test_detect_anomalies_flags_extreme_output(spark):
    timestamp = datetime(2022, 3, 1, 12, 0)

    # Most turbines have similar output, with turbine 15 as an obvious outlier
    data = [
        (timestamp, 1, 3.0),
        (timestamp, 2, 3.1),
        (timestamp, 3, 2.9),
        (timestamp, 4, 3.0),
        (timestamp, 5, 3.1),
        (timestamp, 6, 2.9),
        (timestamp, 7, 3.0),
        (timestamp, 8, 3.1),
        (timestamp, 9, 2.9),
        (timestamp, 10, 3.0),
        (timestamp, 11, 3.1),
        (timestamp, 12, 2.9),
        (timestamp, 13, 3.0),
        (timestamp, 14, 3.1),
        (timestamp, 15, 8.0),
    ]

    df = spark.createDataFrame(
        data,
        ["timestamp", "turbine_id", "power_output"],
    )

    anomalies = detect_anomalies(df).collect()

    assert len(anomalies) == 1
    assert anomalies[0]["turbine_id"] == 15