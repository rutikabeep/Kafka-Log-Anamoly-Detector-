import os

import joblib
import mlflow
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split

from utils.autoencoder import Autoencoder
from utils.settings import AE_LATEST_MODEL_PATH, MODEL_DIR


def train_autoencoder(df_batch, version, device="cpu", epochs=20):
    """
    Train Autoencoder for anomaly detection and log to MLflow.
    Saves model, feature columns, and threshold.
    """
    # Features and labels
    X = df_batch.drop(columns=["BlockId", "Label", "Type"], errors="ignore").astype(
        "float32"
    )
    y = df_batch["Label"].map({"Success": 0, "Fail": 1}).values

    # Save feature columns
    MODEL_DIR.mkdir(exist_ok=True)
    feature_columns_path = MODEL_DIR / f"ae_model_columns_{version}.pkl"
    joblib.dump(list(X.columns), feature_columns_path)

    # Train/val split
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    X_train_norm = X_train[y_train == 0].values
    X_val, y_val = X_val.values, y_val

    # Normalize
    mean, std = X_train_norm.mean(0), X_train_norm.std(0) + 1e-8
    X_train_norm = (X_train_norm - mean) / std
    X_val = (X_val - mean) / std

    # Tensors
    X_train_tensor = torch.tensor(X_train_norm, dtype=torch.float32).to(device)
    X_val_tensor = torch.tensor(X_val, dtype=torch.float32).to(device)

    # Model
    ae = Autoencoder(input_dim=X.shape[1]).to(device)
    criterion = nn.MSELoss()
    optimizer = optim.Adam(ae.parameters(), lr=1e-3)

    # Train
    ae.train()
    for epoch in range(epochs):
        optimizer.zero_grad()
        recon = ae(X_train_tensor)
        loss = criterion(recon, X_train_tensor)
        loss.backward()
        optimizer.step()
        if (epoch + 1) % 5 == 0:
            print(f"Epoch {epoch + 1}, Loss {loss.item():.6f}")

    # Validation
    ae.eval()
    with torch.no_grad():
        recon_val = ae(X_val_tensor)
        errors_val = ((recon_val - X_val_tensor) ** 2).mean(dim=1).cpu().numpy()

    # Threshold
    threshold = np.percentile(errors_val[y_val == 0], 99.5)
    print("Anomaly detection threshold:", threshold)

    # Classification report
    anomaly_pred = (errors_val > threshold).astype(int)
    report = classification_report(y_val, anomaly_pred, digits=4, output_dict=True)
    print(classification_report(y_val, anomaly_pred, digits=4))

    # Save model
    checkpoint = {
        "model_state": ae.state_dict(),
        "mean": mean,
        "std": std,
        "threshold": threshold,
        "input_dim": X.shape[1],
    }
    model_path = MODEL_DIR / f"ae_model_{version}.pt"
    torch.save(checkpoint, model_path)
    torch.save(checkpoint, AE_LATEST_MODEL_PATH)

    # MLflow logging
    os.makedirs("mlruns", exist_ok=True)
    mlflow.set_tracking_uri("file:./mlruns")
    mlflow.set_experiment("hdfs_anomaly_detection")
    with mlflow.start_run(run_name=f"ae_{version}"):
        mlflow.log_param("model_type", "autoencoder")
        mlflow.log_param("epochs", epochs)
        mlflow.log_param("threshold_percentile", 99.5)
        mlflow.log_metric("precision_fail", report["1"]["precision"])
        mlflow.log_metric("recall_fail", report["1"]["recall"])
        mlflow.log_metric("f1_fail", report["1"]["f1-score"])
        mlflow.log_metric("roc_auc", roc_auc_score(y_val, errors_val))

    return ae, report, threshold
