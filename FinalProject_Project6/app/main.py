import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
import pandas as pd
from evaluate import evaluate_baseline, compare_results
from report_generator import generate_report

def main():
    print("Starting PySpark ALS Recommender System...")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Data612Project5") \
        .getOrCreate()

    # Load data from CSV
    data_path = "/data/data.csv"
    ratings_df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Cast user_id, hotel_id, and overall to correct numeric types
    ratings_df = ratings_df.select(
        col("user_id").cast("int"),
        col("hotel_id").cast("int"),
        col("overall").cast("float")
    )

    # Drop rows with nulls after casting
    ratings_df = ratings_df.dropna()

    print(f"Loaded data: {ratings_df.count()} rows")

    # Train-test split
    train, test = ratings_df.randomSplit([0.8, 0.2])

    # ALS model configuration
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="user_id",
        itemCol="hotel_id",
        ratingCol="overall",
        coldStartStrategy="drop",
        nonnegative=True
    )

    # Train ALS model
    model = als.fit(train)

    # Make predictions
    predictions = model.transform(test)

    # Evaluate predictions
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="overall",
        predictionCol="prediction"
    )
    rmse_spark = evaluator.evaluate(predictions)
    print(f"Spark ALS Model RMSE: {rmse_spark:.4f}")

    # Evaluate baseline model
    ratings_pd = ratings_df.toPandas()
    rmse_baseline = evaluate_baseline(ratings_pd)
    print(f"Baseline Model RMSE: {rmse_baseline:.4f}")

    # Compare both results
    compare_results(rmse_spark, rmse_baseline)

    # Generate PDF report
    report_path = "/app/Project5_Report.pdf"
    generate_report(rmse_spark, rmse_baseline, report_path)
    print(f"Report generated at: {report_path}")

    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    main()
