#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import datetime, warnings
warnings.filterwarnings("ignore")

# Data Loading
df1 = pd.read_csv('data.csv')

# Renaming Airlines
df1['OP_CARRIER'].replace({
    'UA':'United Airlines',
    'AS':'Alaska Airlines',
    '9E':'Endeavor Air',
    'B6':'JetBlue Airways',
    'EV':'ExpressJet',
    'F9':'Frontier Airlines',
    'G4':'Allegiant Air',
    'HA':'Hawaiian Airlines',
    'MQ':'Envoy Air',
    'NK':'Spirit Airlines',
    'OH':'PSA Airlines',
    'OO':'SkyWest Airlines',
    'VX':'Virgin America',
    'WN':'Southwest Airlines',
    'YV':'Mesa Airline',
    'YX':'Republic Airways',
    'AA':'American Airlines',
    'DL':'Delta Airlines'
}, inplace=True)

# Dropping Columns
df1 = df1.drop(["Unnamed: 27"], axis=1)

# Handling Cancelled Flights
df1 = df1[(df1['CANCELLED'] == 0)]
df1 = df1.drop(['CANCELLED'], axis=1)

# Handling Cancellation Codes
df1 = df1.drop(["CANCELLATION_CODE"], axis=1)

# Dropping DIVERTED column
df1 = df1.drop(['DIVERTED'], axis=1)

# Dropping Delay Reason Columns
df1 = df1.drop(['CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY'], axis=1)

# Dropping Flight Number
df1 = df1.drop(['OP_CARRIER_FL_NUM'], axis=1)

# Dropping Time Columns
df1.drop(columns=['DEP_TIME', 'ARR_TIME'], inplace=True)

# Handling Missing Values
df1["DEP_DELAY"] = df1["DEP_DELAY"].fillna(0)
df1['TAXI_IN'].fillna((df1['TAXI_IN'].mean()), inplace=True)
df1 = df1.dropna()

# Binning Time Columns
time_columns = ['CRS_DEP_TIME', 'WHEELS_OFF', 'WHEELS_ON', 'CRS_ARR_TIME']
for col in time_columns:
    df1[col] = np.ceil(df1[col] / 600).astype(int)

# Extracting Date Information
df1['DAY'] = pd.DatetimeIndex(df1['FL_DATE']).day
df1['MONTH'] = pd.DatetimeIndex(df1['FL_DATE']).month

import calendar
df1['MONTH_AB'] = df1['MONTH'].apply(lambda x: calendar.month_abbr[x])

# Binary Classification
df1['FLIGHT_STATUS'] = df1['ARR_DELAY'].apply(lambda x: 0 if x < 0 else 1)

# Convert FL_DATE to datetime and extract weekday
df1['FL_DATE'] = pd.to_datetime(df1['FL_DATE'])
df1['WEEKDAY'] = df1['FL_DATE'].dt.dayofweek

# Drop unnecessary columns
df1 = df1.drop(columns=['FL_DATE', 'MONTH_AB', 'ARR_DELAY'])

# Saving the Cleaned Data
df1.to_csv('cleaned.csv', index=False)
