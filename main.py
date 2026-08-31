from pyspark.sql import SparkSession

from wind_turbine.ingestion import read_turbine_data
from wind_turbine.cleaning import clean_turbine_data
from wind_turbine.transformations import calculate_daily_summary
from wind_turbine.anomaly_detection import detect_anomalies
from wind_turbine.storage import write_outputs, create_database


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("WindTurbinePipeline")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        raw_df = read_turbine_data(
            spark,
            "data/input/data_group_*.csv",
        )

        cleaned_df, rejected_df = clean_turbine_data(raw_df)

        summary_df = calculate_daily_summary(cleaned_df)

        anomalies_df = detect_anomalies(cleaned_df)

        write_outputs(
            cleaned_df=cleaned_df,
            rejected_df=rejected_df,
            summary_df=summary_df,
            anomalies_df=anomalies_df,
            output_dir="data/output",
        )

        create_database(
            output_dir="data/output",
            database_path="data/wind_turbine.duckdb",
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()