import math
from itertools import islice
import pandas as pd
import matplotlib.pyplot as plt


def evaluate(dataset_name, dataset_path, output_path):
    dataframe = pd.DataFrame(pd.read_csv(dataset_path, sep=";"))
    elapsed_time = dataframe['time_difference'].sum()
    total_data_income = (dataframe['total_volume'].sum())/(10**3)  # KB
    traffic_mean_vol = (total_data_income/elapsed_time)  # KB/s
    traffic_mean = total_data_income/len(dataframe)  # KB per user
    dataframe['total_volume'] = dataframe['total_volume'].apply(lambda x: x/(10**3))  # KB
    dataframe['ratio_vol_td'] = dataframe['ratio_vol_td'].apply(lambda x: x/(10**3))  # KB/s

    dataframe.insert(loc=0, column='id', value=range(1, len(dataframe) + 1))

    # Sigma over exchanged data
    dataframe['margin_to_mean'] = dataframe['total_volume']
    dataframe['margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: (x - traffic_mean))
    dataframe['squared_margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: (x - traffic_mean)**2)
    sigma_data = math.sqrt(dataframe['squared_margin_to_mean'].sum()/(len(dataframe)-1))

    # Sigma over traffic volume
    dataframe['margin_to_mean_vol'] = dataframe['ratio_vol_td']
    dataframe['margin_to_mean_vol'] = dataframe['ratio_vol_td'].apply(lambda x: (x - traffic_mean_vol))
    dataframe['squared_margin_to_mean_vol'] = dataframe['margin_to_mean_vol'].apply(lambda x: (x - traffic_mean_vol) ** 2)
    sigma_vol = math.sqrt(dataframe['squared_margin_to_mean_vol'].sum() / (len(dataframe) - 1))

    fig, ax = plt.subplots(figsize=(14, 6))
    dataframe.plot('id', 'total_volume', kind='scatter', linewidth='0.5', ax=ax, label='Data exchanged')
    plt.axhline(traffic_mean, color='r', label='Mean')
    plt.axhline(sigma_data, color='g', label='Sigma')
    plt.xlabel('IP ID')
    plt.ylabel('KB')
    plt.legend()
    plt.title("Data Analysis")
    plt.savefig(output_path + dataset_name + "-data_analysis.png", dpi=300)

    fig, ax = plt.subplots(figsize=(14, 6))
    dataframe.plot('id', 'ratio_vol_td', kind='scatter', linewidth='0.5', ax=ax, label='Volume per second')
    plt.axhline(traffic_mean_vol, color='r', label='Mean')
    plt.axhline(sigma_vol, color='g', label='Sigma')
    plt.xlabel('IP ID')
    plt.ylabel('KB/s')
    plt.legend()
    plt.title("Traffic Volume Analysis")
    plt.savefig(output_path + dataset_name + "-volume_analysis.png", dpi=300)

    dataframe.to_csv(output_path + dataset_name + "-indexed", index=False)
