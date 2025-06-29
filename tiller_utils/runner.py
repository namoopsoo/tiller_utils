import argparse
import polars as pl
from pathlib import Path
from tiller_utils.institutions.capitalone import convert_to_tiller as co_convert_to_tiller, agg_amount_type
from tiller_utils.institutions.paypal import convert_to_tiller as paypal_convert_to_tiller
from tiller_utils.date_utils import utc_ts

def bake_options():
    return [
        [['--dry-run', '-D'],
            {'action': 'store_true',
                'help': 'Dry run. Just print the command.'},],

        [['--source-csv', '-s'],
            {'action': 'store',
                'help': 'csv downloaded from institution, like from capital one'},],
        [['--institution', '-i'],
            {'action': 'store',
                'help': 'from institution type'},],
    ]

def get_args():
    parser = argparse.ArgumentParser()

    [parser.add_argument(*x[0], **x[1])
            for x in bake_options()]

    # Collect args from user.
    kwargs = dict(vars(parser.parse_args()))
    return kwargs

def main():
    kwargs = get_args()

    csv_path = Path(kwargs["source_csv"])
    institution = kwargs.get("institution", "capitalone").lower()

    read_kwargs = {}
    if institution == "paypal":
        read_kwargs["dtypes"] = {"Amount": pl.Utf8, "Balance": pl.Utf8}

    raw_df = pl.read_csv(csv_path, **read_kwargs)

    if institution == "capitalone":
        new_df = co_convert_to_tiller(raw_df)
    elif institution == "paypal":
        new_df = paypal_convert_to_tiller(raw_df)
    else:
        raise ValueError(f"Unsupported institution: {institution}")

    # new_df = new_df.drop_nulls()

    new_path = csv_path.with_stem(
        utc_ts()
        + "-"
        + csv_path.stem
        + "--for-tiller"
    )

    new_df.write_csv(new_path)

if __name__ == "__main__":
    main()
