
from Table_to_DF import preprocess_table
from sklearn.pipeline import Pipeline
import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt


class LSTM_COIN:
    def __init__(self):
        self.money = 0
        self.ror = 0
        self.buy_count = 0
        self.df_list = []

    def predict(self):
        trainX,testX = train_test_split(self.df_list[0], test_size= 0.3, random_state=42)
        model = Sequential()
        model.add(LSTM(64, input_shape=(trainX.shape[1], trainX.shape[2]), # (seq length, input dimension)
               return_sequences=True))
        model.add(LSTM(32, return_sequences=False))
        model.summary()
        # specify your learning rate
        learning_rate = 0.01
        # create an Adam optimizer with the specified learning rate
        optimizer = Adam(learning_rate=learning_rate)
        # compile your model using the custom optimizer
        model.compile(optimizer=optimizer, loss='mse')
        prediction = model.predict(testX)
        print(prediction.shape, testX.shape)
test = LSTM_COIN()
test.predict()


