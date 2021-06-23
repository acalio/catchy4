import s3fs
import pyarrow.parquet as pq
import click


@click.command()
@click.option('--bucket', type=str)
@click.option("--folder", type=str)
@click.option("--output", type=str)
def main(bucket, folder, output):
    s3 = s3fs.S3FileSystem()
    s3Root = f'{bucket}/{folder}'
    print(f"Downloading files from {s3Root}")
    parquetObj = pq.ParquetDataset(s3Root, filesystem=s3)
    # convert to pandas dataframe
    table = parquetObj.read()
    df = table.to_pandas()
    df.to_csv(output)


if __name__ == '__main__':
    main()
