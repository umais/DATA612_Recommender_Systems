import os
from flask import Flask, request, jsonify, render_template
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import json
import logging
logging.basicConfig(level=logging.DEBUG)
# Set Python paths before SparkSession is created
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

app = Flask(__name__, template_folder='templates', static_folder='static')

# Initialize Spark session with explicit python config
spark = SparkSession.builder \
    .appName("ALS Recommender API") \
    .config("spark.pyspark.python", "/usr/local/bin/python3") \
    .config("spark.pyspark.driver.python", "/usr/local/bin/python3") \
    .getOrCreate()

MODEL_PATH = "/media/amazonratings/als_model"
als_model = ALSModel.load(MODEL_PATH)

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/predict', methods=['POST'])
def predict():
    try:
        user_id = int(request.form['user_id'])
        item_id = int(request.form['item_id'])
    except (KeyError, ValueError):
        return render_template("index.html", error="Invalid input")

    input_df = spark.createDataFrame([(user_id, item_id)], ["user_id", "item_id"])
    prediction = als_model.transform(input_df).collect()

    if prediction and prediction[0]['prediction'] is not None:
        pred = round(prediction[0]['prediction'], 4)
        return render_template("index.html", prediction=pred, user_id=user_id, item_id=item_id)
    else:
        return render_template("index.html", error="No prediction available")
    


@app.route('/top-recommendations', methods=['GET'])
def top_recommendations():
    path = "/media/amazonratings/top_10_recommendations.json/part-00000-3d135d8a-7883-4cdd-8685-6e1aa11b3a94-c000.json"
    try:
        data = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:  # skip empty lines
                    data.append(json.loads(line))
        return render_template("top_recommendations.html", recommendations=data)
    except Exception as e:
        logging.error("Failed to load recommendations", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/hello', methods=['GET'])
def hello():
    return jsonify({"message": "Hello, world!"})

@app.route('/api/predict', methods=['POST'])
def api_predict():
    data = request.get_json()

    if not data or not all(k in data for k in ('user_id', 'item_id')):
        return jsonify({'error': 'user_id and item_id are required'}), 400

    try:
        user_id = int(data['user_id'])
        item_id = int(data['item_id'])
    except ValueError:
        return jsonify({'error': 'user_id and item_id must be integers'}), 400

    input_df = spark.createDataFrame([(user_id, item_id)], ["user_id", "item_id"])
    prediction = als_model.transform(input_df).collect()

    if prediction and prediction[0]['prediction'] is not None:
        pred = prediction[0]['prediction']
        return jsonify({'user_id': user_id, 'item_id': item_id, 'prediction': round(pred, 4)})
    else:
        return jsonify({'error': 'No prediction available'}), 404

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, debug=True)
