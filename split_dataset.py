import pandas as pd
from sklearn.model_selection import train_test_split

from utils.settings import EVENT_MATRIX_PATH, TEST_MATRIX_PATH, TRAIN_MATRIX_PATH


df = pd.read_csv(EVENT_MATRIX_PATH)

# Split 70% train, 30% test (stratify on Label for balanced classes)
train_df, test_df = train_test_split(
    df,
    test_size=0.3,
    random_state=42,
    stratify=df["Label"],  # ensures Fail/Success ratio stays the same
)

# Save to CSV
TRAIN_MATRIX_PATH.parent.mkdir(exist_ok=True)
train_df.to_csv(TRAIN_MATRIX_PATH, index=False)
test_df.to_csv(TEST_MATRIX_PATH, index=False)

print(f"Train shape: {train_df.shape}")
print(f"Test shape: {test_df.shape}")
