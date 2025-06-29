import argparse
import polars as pl
from pathlib import Path
from tiller_utils.institutions.capitalone import convert_to_tiller, agg_amount_type
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

    capital_one_csv_path = Path(kwargs["source_csv"])
    capital_one_raw_df = convert_to_tiller(
        pl.read_csv( capital_one_csv_path )
    ).drop_nulls()

    new_path = capital_one_csv_path.with_stem(
        utc_ts()
        + "-" 
        + capital_one_csv_path.stem
        + "--for-tiller"
    )

    (capital_one_raw_df
 .write_csv((new_path))
)

if __name__ == "__main__":
    main()