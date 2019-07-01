import math
from numpy import percentile
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

    # Percentile Traffic
    q25, q75 = percentile(dataframe['ratio_vol_td'], 25), percentile(dataframe['ratio_vol_td'], 75)
    inter_quartile_range = q75 - q25
    # calculate the outlier cutoff
    cut_off = inter_quartile_range * 1.5
    lower, upper = q25 - cut_off, q75 + cut_off
    # identify outliers
    traffic_outliers = [x for x in dataframe['ratio_vol_td'] if x < lower or x > upper]

    # Percentile Data
    q25, q75 = percentile(dataframe['total_volume'], 25), percentile(dataframe['total_volume'], 75)
    inter_quartile_range = q75 - q25
    # calculate the outlier cutoff
    cut_off = inter_quartile_range * 1.5
    lower, upper = q25 - cut_off, q75 + cut_off
    # identify outliers
    data_outliers = [x for x in dataframe['total_volume'] if x < lower or x > upper]

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

    file = open(output_path + dataset_name + "-report", 'a+')
    file.write("Data outliers (" + str(len(data_outliers)) + "): " + str(data_outliers) + "\n")
    file.write("Traffic outliers:(" + str(len(traffic_outliers)) + "): " + str(traffic_outliers) + "\n")
    file.close()

    dataframe.to_csv(output_path + dataset_name + "-indexed", index=False)
