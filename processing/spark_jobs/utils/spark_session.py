# processing/spark_jobs/utils/spark_session.py

# ─────────────────────────────────────────────────────────────────────
# WHAT IS A SPARKSESSION?
#
# SparkSession is the entry point to all PySpark functionality.
# You must create one before doing anything with Spark.
# Think of it like a database connection — you need it open to work.
#
# One SparkSession per application (Spark manages this internally).
# ─────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
import os


def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a configured SparkSession.

    Configuration explained:
    ─────────────────────────────────────────────────────────────────
    .master("local[*]")
        local = run on this single machine (no cluster needed)
        [*] = use ALL available CPU cores on your laptop
        For production: .master("spark://cluster-host:7077")
        The code you write here is 100% identical to production code.

    Delta Lake configs:
        These tell Spark that Delta Lake is installed and should be
        used as the default table format. Without these, Spark cannot
        read or write Delta tables.

    S3 configs:
        These tell Spark how to connect to AWS S3.
        Spark uses the s3a:// protocol (not s3://) for better performance.
        s3a = S3A FileSystem = optimised S3 connector for Hadoop/Spark.

    .config("spark.jars.packages", ...)
        Spark downloads these Java libraries automatically from Maven
        (Maven is Java's package repository, like PyPI for Python).
        First download: 3-5 minutes. After that: instant (cached).
    ─────────────────────────────────────────────────────────────────

    Args:
        app_name: name shown in Spark logs (use descriptive names)

    Returns:
        SparkSession: ready-to-use Spark session
    """

    # Read AWS credentials set by `aws configure` in Part A
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

    # These JAR packages are the Java libraries Spark needs
    # They are downloaded automatically the first time you run Spark
    spark_packages = ",".join([
        # Kafka connector: lets Spark read/write Kafka topics
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        # Delta Lake: ACID transactions on S3
        "io.delta:delta-spark_2.12:3.3.0",
        # AWS S3A FileSystem: lets Spark read/write S3
        "org.apache.hadoop:hadoop-aws:3.3.4",
        # AWS SDK: core AWS Java library (required by hadoop-aws)
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # ── Delta Lake configuration ──────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # ── AWS S3 configuration ──────────────────────────────────
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{aws_region}.amazonaws.com"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        # Path-style access: required for eu-west-1 and other non-US regions
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Increase connection timeout for slower network conditions
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")

        # ── Java package downloads ────────────────────────────────
        .config("spark.jars.packages", spark_packages)

        # ── Reduce noise in logs (only show WARN and ERROR) ───────
        .config("spark.log.level", "WARN")

        .getOrCreate()
    )

    # Set log level on the SparkContext (different from config above)
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n✅ SparkSession created: '{app_name}'")
    print(f"   Spark version : {spark.version}")
    print(f"   Python version: {spark.sparkContext.pythonVer}")
    print(f"   Running in    : local mode (all CPU cores)\n")

    return spark