import polars as pl
from rich import print

path = "/home/amh1124/groups/grp_quant/database/production/assets/*.parquet"

print(
    pl.scan_parquet(path)
    .filter(
        pl.col('date').eq(pl.col('date').max()),
        pl.col('in_universe'),
        pl.col('ticker').is_not_null()
    )
    .sort('barrid')
    .collect()
)