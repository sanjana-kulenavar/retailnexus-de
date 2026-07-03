from pyspark.sql import DataFrame, SparkSession


def write_delta(df, path, mode="append", partition_by=None):
    """Writes a DataFrame as a Delta Lake table to S3."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    print(f"   Delta write complete: {path} (mode={mode})")


def read_delta(spark, path):
    """Reads a Delta Lake table from S3 into a DataFrame."""
    return spark.read.format("delta").load(path)


def read_delta_version(spark, path, version):
    """Reads a specific past version of a Delta table (time travel)."""
    return spark.read.format("delta").option("versionAsOf", version).load(path)
