import s3fs
import pyarrow.parquet as pq
import click


@click.command()
@click.option("--folder", type=str)
@click.option("--output", type=str)
def main(folder, output):
    parquetObj = pq.ParquetDataset(folder)#filesystem=s3)
    # convert to pandas dataframe
    table = parquetObj.read()
    df = table.to_pandas()
    df.to_csv(output)


if __name__ == '__main__':
    main()
