from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define the Hadoop configuration properties
conf = {
    "fs.default.name": "hdfs://localhost:9000",
    "dfs.replication": "1",
    "dfs.namenode.name.dir": "/usr/local/hadoop/data/namenode",
    "dfs.datanode.data.dir": "/usr/local/hadoop/data/datanode",
    "dfs.permissions.enabled": "false"
}

# Create an HDFS client
hdfs_client = InsecureClient("http://localhost:50070", user="hadoop", **conf)

# Define the path for the data lake directory
data_lake_path = "/data_lake"

# Create the data lake directory if it doesn't exist
if not hdfs_client.status(data_lake_path, strict=False):
    hdfs_client.makedirs(data_lake_path)

# Define the paths for the data sources
sales_data_path = "/sales_data"
customer_data_path = "/customer_data"
product_data_path = "/product_data"

# Load the data sources into Spark dataframes
spark = SparkSession.builder.appName("DataLakeBuilder").getOrCreate()

sales_data = spark.read.csv(sales_data_path, header=True, inferSchema=True)
customer_data = spark.read.csv(customer_data_path, header=True, inferSchema=True)
product_data = spark.read.csv(product_data_path, header=True, inferSchema=True)

# Merge the data sources into a single dataframe
merged_data = sales_data \
    .join(customer_data, on="customer_id") \
    .join(product_data, on="product_id") \
    .select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("price"),
        col("date"),
        col("customer_name"),
        col("customer_email"),
        col("product_name"),
        col("product_category")
    )

# Write the merged data to the data lake directory in Parquet format
merged_data.write.mode("overwrite").parquet(data_lake_path)

# Create an external Hive table for the data lake
hive_table_name = "customer_orders"
hive_table_location = data_lake_path

hive_query = f"""
CREATE EXTERNAL TABLE {hive_table_name} (
    order_id INT,
    customer_id INT,
    product_id INT,
    price DECIMAL(10,2),
    date DATE,
    customer_name STRING,
    customer_email STRING,
    product_name STRING,
    product_category STRING
)
STORED AS PARQUET
LOCATION '{hive_table_location}'
"""

spark.sql(hive_query)
