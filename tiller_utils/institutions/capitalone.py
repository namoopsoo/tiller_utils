import polars as pl
from datetime import datetime
from tiller_utils.date_utils import utc_ts

def convert_to_tiller(df, account_id_map):
    """
    Example
    -------
    >>> df = pl.DataFrame({
    ...     "Transaction Date": ["2024-12-28", "2025-01-02"],
    ...     "Description": ["Store Purchase", "Refund"],
    ...     "Debit": [50.0, 0.0],
    ...     "Credit": [0.0, 25.0],
    ...     "Card No.": ["1234", "5678"]
    ... })
    """

    empty_columns = [
        "Category", "Metadata", "Full Description", "Categorized Date", "Check Number", 
        # also empty? #TODO or not empty?
        "Month", "Week",
    ]


    # Transform to Tiller-style columns
    df_tiller = (
        df
        # 1) Convert "Transaction Date" -> "Date" in MM/DD/YYYY format
        .with_columns(
            # pl.col("Transaction Date")
            pl.col("Posted Date")
            #.str.strptime(pl.Date, format="%Y-%m-%d")    # parse as date
            # .dt.strftime("%m/%d/%Y")                     # reformat
            .alias("Date")
        )
        # 2) Merge "Debit" and "Credit" into one "Amount" column
        #    (assuming one is always 0; subtracting debit to make it negative)
        .with_columns(
            pl.col("Credit").fill_null(0),
            pl.col("Debit").fill_null(0)
        )
        .with_columns(
            (pl.col("Credit") - pl.col("Debit"))
            .alias("Amount")
        )
        # 3) Convert "Card No." -> "Account #", prefixing with "xxxx"
        .with_columns(
            (pl.lit("xxxx") + pl.col("Card No.").cast(pl.Utf8))
            .alias("Account #")
        )
        .with_row_index()
        .with_columns(
            (pl.lit(f"manual:{utc_ts()}--")
             + pl.col("index").cast(pl.String)).alias("Transaction ID")
        )
        .drop("index")
        # Optionally remove the old columns you no longer need:
        #.drop(["Transaction Date", "Card No."])
        .with_columns(pl.lit("Capital One").alias("Institution"))
        .with_columns(
            pl.col("Account #").map_elements(
                lambda x: f"Cap One {x}", return_dtype=pl.String)
                .alias("Account")
        )
        .with_columns(
            # (pl.col("Account #").map_elements(
            #     lambda x: account_id_map[x], return_dtype=pl.String)
            #     .alias("Account ID"))
            (
                pl.col("Account #")
                .replace_strict(account_id_map).alias("Account ID")))
        .with_columns(
            pl.lit(datetime.today()).alias("Date Added"),
        )
        # empties
        .with_columns(
            *[
                pl.lit(None).alias(col)
                for col in empty_columns
            ]
            #pl.lit(None).alias("Category"),
        )
        .drop(
            ["Credit", "Debit", "Card No.", "Posted Date", "Transaction Date"])
        .select(
            "Date", "Description", "Category", "Amount", "Account", "Account #", "Institution", "Month", "Week", "Transaction ID", "Account ID", "Check Number", "Full Description", "Date Added", "Metadata",
        )
    )
    
    return df_tiller


def agg_amount_type(df):
    """aggregate by credit and debit
    """
    return (df
      .with_columns(
          pl.when(
              pl.col("Amount") >= 0
          ).then(pl.lit("credit")).otherwise(pl.lit("debit")).alias("Type")
      )
      .group_by("Type").agg(pl.col("Amount").sum()))