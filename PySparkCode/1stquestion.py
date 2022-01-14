from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder \
     .appName("Streaming Word Count") \
     .master("local[3]") \
     .getOrCreate()

    customer_orders_temp = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'False') \
        .option('inferSchema', 'True') \
        .load('hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/customer_orders_temp')

    result = customer_orders_temp.count()

    data = [(result,)]
    dataSchema = StructType([
            StructField('count', IntegerType(), True)
            ])
    result_df = spark.createDataFrame(data=data, schema = dataSchema)

    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/pizza_order_count")