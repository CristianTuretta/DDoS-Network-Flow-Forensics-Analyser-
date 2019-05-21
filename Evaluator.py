import math
from itertools import islice
import pandas as pd
import matplotlib.pyplot as plt


def evaluate(dataset_name, dataset_path, output_path):
    dataframe = pd.DataFrame(pd.read_csv(dataset_path, sep=";"))
    elapsed_time = dataframe['time_difference'].sum()
    total_data_income = (dataframe['total_volume'].sum())/(10**6)  # MB
    traffic_mean_vol = (total_data_income/elapsed_time)  # MB/s
    traffic_mean = total_data_income/len(dataframe)  # MB per user
    dataframe['total_volume'] = dataframe['total_volume'].apply(lambda x: x/(10**6))  # MB
    dataframe['ratio_vol_td'] = dataframe['ratio_vol_td'].apply(lambda x: x/(10**6))  # MB/s

    dataframe.insert(loc=0, column='id', value=range(1, len(dataframe) + 1))

    dataframe['margin_to_mean'] = dataframe['total_volume']
    dataframe['margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: (x - traffic_mean_vol))

    dataframe['squared_margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: (x - traffic_mean_vol)**2)
    sigma = math.sqrt(dataframe['squared_margin_to_mean'].sum()/(len(dataframe)-1))

    fig, ax = plt.subplots(figsize=(14, 6))
    dataframe.plot('id', 'total_volume', kind='scatter', linewidth='0.5', ax=ax, label='Data exchanged')
    plt.axhline(traffic_mean, color='r', label='Mean')
    plt.xlabel('IP ID')
    plt.ylabel('MB')
    plt.savefig(output_path + dataset_name + "-data_analysis.png", dpi=300)

    fig, ax = plt.subplots(figsize=(14, 6))
    dataframe.plot('id', 'ratio_vol_td', kind='scatter', linewidth='0.5', ax=ax, label='Volume per second')
    plt.axhline(traffic_mean_vol, color='g', label='Mean')
    plt.xlabel('IP ID')
    plt.ylabel('MB/s')
    plt.savefig(output_path + dataset_name + "-volume_analysis.png", dpi=300)

    fig, ax = plt.subplots(figsize=(14, 6))
    dataframe.plot('id', 'squared_margin_to_mean', kind='scatter', linewidth='0.5', ax=ax, label='Squared margin')
    plt.axhline(sigma**2, color='r', label='Sigma')
    plt.xlabel('IP ID')
    plt.ylabel('Squared Margin from mean (MB/s)')
    plt.savefig(output_path + dataset_name + "-squared_margin_analysis.png", dpi=300)

    dataframe.to_csv(output_path + dataset_name + "-indexed", index=False)
