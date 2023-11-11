import lightgbm as lgb
import pandas as pd
import numpy as np
import os
import pickle
import json
import sys
import time
import mlflow


def load_data(path):
    df = pd.read_csv(path)
    return df


def load_model(path):
    with open(path, "rb") as f:
        model = pickle.load(f)
    return model


def predict(model, df):
    y_pred = model.predict(df)
    return y_pred
