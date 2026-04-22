import pandas as pd

from utils.train import train_model
from utils.settings import MODEL_VERSION, TRAIN_MATRIX_PATH


df = pd.read_csv(TRAIN_MATRIX_PATH)
train_model(df, version=MODEL_VERSION)

print("Training done. Model saved and metrics logged to MLflow.")
