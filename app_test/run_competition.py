# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


if __name__ == '__main__':

    def app():
        
        spark = SparkSession.builder.appName("MinIOIntegration").getOrCreate()
        
        s3_bucket_origin = "s3a://datalakefutebol-landingzone"
        s3_bucket_destiny = "s3a://datalakefutebol-bronze/"
        # Caminhos de entrada e saída no MinIO
        input_path = f"{s3_bucket_origin}/datalakefutebol-landingzone/competition/competitions.json"
        output_path = f"{s3_bucket_destiny}/datalakefutebol-bronze/competition"
        # Read a JSON file from an MinIO bucket using the access key, secret key, 
        # and endpoint configured above
        df_competition = spark.read.option("multiline", "true").json(input_path)
        df_competition_2 = df_competition.withColumn("date_reference_extraction", F.lit(20240819))
        df_competition_2.write.mode("append").format("json").partitionBy("date_reference_extraction").save(
            output_path
        )
    app()
