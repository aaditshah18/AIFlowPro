import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from xgboost import XGBClassifier
import warnings
import pickle

warnings.filterwarnings("ignore")

# Data Loading
df = pd.read_csv('cleaned.csv')

# Encode the target variable
le = LabelEncoder()
df['FLIGHT_STATUS'] = le.fit_transform(df['FLIGHT_STATUS'])

# Define features (X) and target (y)
y = df['FLIGHT_STATUS']
X = df.drop(['FLIGHT_STATUS', 'DEP_DELAY'], axis=1)

# Identify categorical and numerical columns
categorical_cols = ['OP_CARRIER', 'ORIGIN', 'DEST']
numerical_cols = X.columns.difference(categorical_cols)

# Preprocessing pipelines for both numerical and categorical data
numerical_transformer = StandardScaler()
categorical_transformer = OneHotEncoder(handle_unknown='ignore')

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numerical_cols),
        ('cat', categorical_transformer, categorical_cols)
    ])

# Apply the transformations to the features
X_preprocessed = preprocessor.fit_transform(X)

# Split the dataset
X_train, X_test, y_train, y_test = train_test_split(X_preprocessed, y, test_size=0.25, random_state=42)

# Train the XGBoost model
clf = XGBClassifier(random_state=42, use_label_encoder=False, eval_metric='logloss')
clf.fit(X_train, y_train)
training_preds = clf.predict(X_train)
val_preds = clf.predict(X_test)
training_accuracy = accuracy_score(y_train, training_preds)
val_accuracy = accuracy_score(y_test, val_preds)

print("Training Accuracy: {:.2f}%".format(training_accuracy * 100))
print("Validation Accuracy: {:.2f}%".format(val_accuracy * 100))

print("Confusion Matrix:\n", confusion_matrix(y_test, val_preds))
print("Classification Report:\n", classification_report(y_test, val_preds))

# Save the model and the preprocessor to disk
with open('flight_delay_model_xgb.pkl', 'wb') as model_file:
    pickle.dump(clf, model_file)

with open('preprocessor.pkl', 'wb') as preprocessor_file:
    pickle.dump(preprocessor, preprocessor_file)

print("Model and preprocessor saved.")