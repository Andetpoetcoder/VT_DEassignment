from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Khoi tao SparkSession
spark = SparkSession.builder \
    .appName("HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Chuan hoa du lieu
def new_date_convert(date):
    original_date = datetime.strptime(date, "%m/%d/%Y")
    return original_date.strftime("%Y%m%d")

to_new_date_udf = udf(new_date_convert, StringType())

# Define schema
student_schema = StructType([
    StructField("date", StringType(), True),
    StructField("student_code", IntegerType(), True),
    StructField("student_name", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("totalFile", IntegerType(), True),
])
activity_schema = StructType([
    StructField("student_code", IntegerType(), True),
    StructField("activity", StringType(), True),
    StructField("numberOfFile", IntegerType(), True),
    StructField("time", StringType(), True)
])

danhsachsv_schema = StructType([
    StructField("student_code", IntegerType(), True),
    StructField("student_name", StringType(), True),
])

# Doc file tu HDFS
df_act = spark.read.schema(activity_schema).parquet("hdfs://namenode/raw_zone/fact/activity")
    
list_of_student = spark.read.schema(danhsachsv_schema).csv("hdfs://namenode/raw_zone/fact/student_list")

# Xu ly du lieu
df_act = df_act.withColumn("date", to_new_date_udf(col("time"))).join(list_of_student, "student_code").select("date", "student_code", "student_name", "activity", "numberOfFile")
df_act = df_act.groupBy("date", "student_code", "student_name", "activity").agg({"numberOfFile": "sum"}).withColumnRenamed("sum(numberOfFile)", "totalFile").orderBy("date")
df_act = df_act.select("date", "student_code", "student_name", "activity", "totalFile")

# Xuat ket qua ra vung output
ds_sv = list_of_student.select("student_name").rdd.flatMap(lambda x: x).collect()

for student_name in ds_sv:
    partition_df = df_act.filter(df_act.student_name == student_name)
    file_name = student_name
    partition_df.coalesce(1).write.mode("overwrite").option("header", "false").csv(f"hdfs://namenode/output/{file_name}.csv")

