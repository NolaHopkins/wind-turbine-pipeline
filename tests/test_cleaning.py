from datetime import datetime

from wind_turbine.cleaning import clean_turbine_data


def test_clean_turbine_data_rejects_invalid_rows(spark):
    # One valid row followed by three rows with different data quality issues
    data = [
        (datetime(2022, 3, 1, 0, 0), 1, 10.0, 180, 3.0),
        (datetime(2022, 3, 1, 1, 0), 1, -1.0, 180, 3.0),
        (datetime(2022, 3, 1, 2, 0), 1, 10.0, 400, 3.0),
        (datetime(2022, 3, 1, 3, 0), 1, 10.0, 180, None),
    ]

    df = spark.createDataFrame(
        data,
        [
            "timestamp",
            "turbine_id",
            "wind_speed",
            "wind_direction",
            "power_output",
        ],
    )

    cleaned_df, rejected_df = clean_turbine_data(df)

    assert cleaned_df.count() == 1
    assert rejected_df.count() == 3

    reasons = {
        row["rejection_reason"]
        for row in rejected_df.select("rejection_reason").collect()
    }

    assert reasons == {
        "invalid_wind_speed",
        "invalid_wind_direction",
        "missing_power_output",
    }