from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("ADLS_Check_Local")\
    .config("spark.jars.packages", "com.azure:azure-storage-file-datalake:12.19.0,org.apache.hadoop:hadoop-azure:3.3.4")\
    .getOrCreate()

storage_account = "data06"
container = "data"
account_key = "vQkBtEnEZEibHMT/Z1Wk/KgexMGbaUyIRlYO2gxW+bSFNnL1QsNFOh/BFG2AQJyLYimcZJouMm1I+AStn1th8Q=="


# Configure Spark to access your container
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    account_key
)
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    account_key
)
yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime("%Y%m%d")   # to fetch all files from yesterday

# Path to the folder
source_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/News_API/Bronze_NewsData/{yesterday}/"

# Read all JSON files in that folder
try:
    df = spark.read.json(source_path + "News_*.json")
except Exception as e:
    print(e)
    sys.exit(1)

#df.printSchema()
df = df.withColumn("PublishTime", to_timestamp("publishedAt","yyyy-MM-dd'T'HH:mm:ss'Z'"))\
    .withColumn("Source_name",col("source.name"))\
    .withColumn("Source_ID", col("source.id"))
# current value: 2025-08-15T23:55:02Z

df = df.select("author","content","description","title","url","urlToImage","PublishTime","Source_name","Source_ID")

print("Modifications have been performed!")


#file_name = datetime.now().strftime("%Y_%m_%d")
output_path =       f"abfss://{container}@{storage_account}.dfs.core.windows.net/News_API/Silver_NewsData1/{yesterday}/"
output_path_wasbs = f"wasbs://{container}@{storage_account}.blob.core.windows.net/News_API/Silver_NewsData/{yesterday}/"


#df.write.mode("overwrite").parquet(output_path )  
df.write.mode("overwrite").parquet(output_path_wasbs)  
print("File have been saved in Silver layer!!")
