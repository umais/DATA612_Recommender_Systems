from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import datetime

def generate_report(rmse_spark, rmse_baseline, filepath):
    c = canvas.Canvas(filepath, pagesize=letter)
    width, height = letter

    # Title
    c.setFont("Helvetica-Bold", 20)
    c.drawCentredString(width / 2, height - 50, "DATA 612 Project 5 Report")

    # Intro
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 100, f"Date: {datetime.date.today().isoformat()}")
    c.drawString(50, height - 120, "Implemented a PySpark ALS Recommender System inside Docker.")

    # Setup
    c.drawString(50, height - 160, "1. Setup & Implementation")
    c.drawString(70, height - 180, "- Python 3.9 with PySpark 3.4.1")
    c.drawString(70, height - 200, "- Docker container with Java 11 and Spark 3.4.1")
    c.drawString(70, height - 220, "- Ratings dataset loaded from CSV")

    # Evaluation
    c.drawString(50, height - 260, "2. Model Evaluation")
    c.drawString(70, height - 280, f"- Spark ALS Model RMSE: {rmse_spark:.4f}")
    c.drawString(70, height - 300, f"- Baseline (mean rating) RMSE: {rmse_baseline:.4f}")

    improvement = ((rmse_baseline - rmse_spark) / rmse_baseline) * 100
    c.drawString(70, height - 320, f"- Improvement over baseline: {improvement:.2f}%")

    # Conclusion
    c.drawString(50, height - 360, "3. Conclusion")
    conclusion_text = (
        "The PySpark ALS model provides a significant improvement over the baseline.\n"
        "This Dockerized setup allows for easy deployment of the recommender system.\n"
        "While local single-node Spark is suitable for moderate data sizes,\n"
        "moving to a distributed Spark cluster becomes necessary for\n"
        "larger datasets and scalability."
    )
    text_object = c.beginText(50, height - 380)
    text_object.setFont("Helvetica", 12)
    for line in conclusion_text.split('\n'):
        text_object.textLine(line)
    c.drawText(text_object)

    # Code Snippet
    c.drawString(50, height - 460, "4. Code Snippet from main.py")

    code_snippet = """
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS

spark = SparkSession.builder.appName("Data612Project5").getOrCreate()
ratings_df = spark.read.csv("/data/data.csv", header=True, inferSchema=True)
ratings_df = ratings_df.select("user_id", "hotel_id", "overall")
als = ALS(maxIter=10, regParam=0.1, userCol="user_id", itemCol="hotel_id", ratingCol="overall")
model = als.fit(ratings_df)
    """

    text_obj = c.beginText(50, height - 480)
    text_obj.setFont("Courier", 8)
    for line in code_snippet.strip().split('\n'):
        text_obj.textLine(line)
    c.drawText(text_obj)

    # Finalize PDF
    c.save()
