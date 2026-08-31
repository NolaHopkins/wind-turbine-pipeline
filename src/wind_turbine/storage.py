import duckdb
from pyspark.sql import DataFrame


def write_outputs(
    cleaned_df: DataFrame,
    rejected_df: DataFrame,
    summary_df: DataFrame,
    anomalies_df: DataFrame,
    output_dir: str,
) -> None:
    """
    Write the processed Spark DataFrames to Parquet.
    """

    outputs = {
        "cleaned_measurements": cleaned_df,
        "rejected_measurements": rejected_df,
        "daily_turbine_summary": summary_df,
        "turbine_anomalies": anomalies_df,
    }

    # Parquet provides a simple intermediate format between Spark and DuckDB
    for name, df in outputs.items():
        df.write.mode("overwrite").parquet(
            f"{output_dir}/{name}"
        )


def create_database(
    output_dir: str,
    database_path: str,
) -> None:
    """
    Load the Parquet outputs into a local DuckDB database.
    """

    tables = [
        "cleaned_measurements",
        "rejected_measurements",
        "daily_turbine_summary",
        "turbine_anomalies",
    ]

    with duckdb.connect(database_path) as connection:
        for table in tables:
            connection.execute(
                f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT *
                FROM read_parquet(
                    '{output_dir}/{table}/*.parquet'
                )
                """
            )