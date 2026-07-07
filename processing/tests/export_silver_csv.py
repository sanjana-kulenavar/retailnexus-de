import os
import sys
sys.path.insert(0, os.path.join(os.getcwd(), "processing", "spark_jobs"))
from utils.spark_session import get_spark_session
from utils.delta_helpers import read_delta
from dotenv import load_dotenv

load_dotenv()
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")
SILVER_PATH = f"s3a://{S3_BUCKET_SILVER}/sales_transactions/"

spark = get_spark_session("export-silver-csv")
df = read_delta(spark, SILVER_PATH)

# Convert to pandas and write a single local CSV
pdf = df.toPandas()
pdf.to_csv("/workspaces/retailnexus-de/silver_export.csv", index=False)
print(f"Exported {len(pdf):,} rows to silver_export.csv")
spark.stop()
