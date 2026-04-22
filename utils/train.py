import os

import joblib
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

from utils.settings import MODEL_DIR, RF_LATEST_MODEL_PATH


def train_model(df_batch, version, n_estimators=100):
    """
    Train RandomForest on a batch of data and log to MLflow.
    Returns trained model, classification report, and saves feature columns for inference.
    """
    # Features and labels
    X = df_batch.drop(columns=["BlockId", "Label", "Type"], errors="ignore").astype(
        "float64"
    )
    y = df_batch["Label"].map({"Success": 0, "Fail": 1})

    # Save feature column names for inference
    MODEL_DIR.mkdir(exist_ok=True)
    feature_columns_path = MODEL_DIR / f"model_columns_{version}.pkl"
    joblib.dump(list(X.columns), feature_columns_path)
    print(f"Saved feature columns for inference: {feature_columns_path}")

    # Train-test split
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # RandomForest
    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X_train, y_train)

    # Validation
    y_pred = clf.predict(X_val)
    report = classification_report(y_val, y_pred, digits=4, output_dict=True)
    print(classification_report(y_val, y_pred, digits=4))

    # Save model
    model_path = MODEL_DIR / f"model_{version}.joblib"
    joblib.dump(clf, model_path)
    joblib.dump(clf, RF_LATEST_MODEL_PATH)

    # Log to MLflow
    os.makedirs("mlruns", exist_ok=True)
    mlflow.set_tracking_uri("file:./mlruns")
    mlflow.set_experiment("hdfs_anomaly_detection")
    with mlflow.start_run(run_name=f"rf_{version}"):
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("class_weight", "balanced")
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("val_size", len(X_val))
        mlflow.log_metric("precision_fail", report["1"]["precision"])
        mlflow.log_metric("recall_fail", report["1"]["recall"])
        mlflow.log_metric("f1_fail", report["1"]["f1-score"])
        mlflow.sklearn.log_model(
            clf,
            name="random_forest_model",
            input_example=X_train.iloc[:1],
        )

    return clf, report
