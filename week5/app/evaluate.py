import numpy as np
from sklearn.metrics import mean_squared_error

def evaluate_baseline(df):
    """
    Baseline RMSE: Predict the mean rating for all entries
    """
    mean_rating = df['overall'].mean()
    mse = mean_squared_error(df['overall'], [mean_rating] * len(df))
    rmse = np.sqrt(mse)
    return rmse

def compare_results(rmse_spark, rmse_baseline):
    print("\n--- Comparison ---")
    print(f"RMSE Spark ALS Model: {rmse_spark:.4f}")
    print(f"RMSE Baseline Model: {rmse_baseline:.4f}")
    improvement = ((rmse_baseline - rmse_spark) / rmse_baseline) * 100
    print(f"Improvement over baseline: {improvement:.2f}%")

