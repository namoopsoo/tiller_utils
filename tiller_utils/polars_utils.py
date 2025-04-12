import polars as pl
from typing import List

def full_outer_anti_join(df1, df2, on_list: List):
    # Anti join from left side
    left_anti = df1.join(df2, on=on_list, how="anti")

    # Anti join from right side
    right_anti = df2.join(df1, on=on_list, how="anti")

    # Combine the results
    result = pl.concat([left_anti, right_anti], how="vertical")
    return result

def drop_if_all_nulls(df):
    return df.filter(~pl.all_horizontal(pl.all().is_null()))

def zero_pad_dates(df, date_cols):
    #     return df.with_columns(
    #             # pl.col("Transaction Date")
    #             pl.col("Posted Date")
    #             .str.strptime(pl.Date, format="%Y-%m-%d")    # parse as date
    #             .dt.strftime("%m/%d/%Y")                     # reformat
    #             .alias("Date")
    #         )
    ...