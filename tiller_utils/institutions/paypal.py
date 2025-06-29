import polars as pl
from datetime import datetime
from tiller_utils.date_utils import utc_ts


def convert_to_tiller(df):
    """Convert a PayPal CSV to Tiller-friendly format"""
    empty_columns = [
        "Category", "Metadata", "Full Description", "Categorized Date", "Check Number",
        "Month", "Week",
    ]

    df_tiller = (
        df
        .filter(~pl.col("Type").is_in(["Bank Deposit to PP Account ", "General Authorization"]))
        .with_columns(
            pl.col("Date").alias("Date"),
            pl.col("Name").alias("Description"),
            pl.col("Amount").cast(pl.Float64).alias("Amount"),
            pl.lit("PayPal").alias("Account #"),
        )
        .with_row_index()
        .with_columns(
            (pl.lit(f"manual:{utc_ts()}--") + pl.col("index").cast(pl.String)).alias("Transaction ID")
        )
        .drop("index")
        .with_columns(
            pl.lit("PayPal").alias("Institution"),
            pl.lit("PayPal").alias("Account"),
            pl.lit("PayPal").alias("Account ID"),
            pl.lit(datetime.today()).alias("Date Added"),
        )
        .with_columns(
            *[pl.lit(None).alias(col) for col in empty_columns]
        )
        .drop(["Time", "TimeZone", "Type", "Status", "Currency", "Receipt ID", "Balance", "Name"])
        .select(
            "Date", "Description", "Category", "Amount", "Account", "Account #",
            "Institution", "Month", "Week", "Transaction ID", "Account ID", "Check Number",
            "Full Description", "Date Added", "Metadata",
        )
    )
    return df_tiller
