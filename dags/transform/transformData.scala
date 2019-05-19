val people_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://<s3-bucket>/people.csv")

people_df.write.mode("overwrite").parquet("s3://<s3-bucket>/people-parquet/people/")