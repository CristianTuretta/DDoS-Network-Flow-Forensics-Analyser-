import math
from itertools import islice
import pandas as pd
import matplotlib.pyplot as plt


def evaluate(dataset_path, save_csv_path):
    dataframe = pd.DataFrame(pd.read_csv(dataset_path, sep=";"))
    elapsed_time = dataframe['time_difference'].sum()
    total_data_income = dataframe['total_volume'].sum()
    traffic_mean = total_data_income/elapsed_time

    dataframe.insert(loc=0, column='id', value=range(1, len(dataframe) + 1))

    dataframe['margin_to_mean'] = dataframe['total_volume']
    dataframe['margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: x - traffic_mean)

    dataframe['squared_margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: (x - traffic_mean)**2)
    sigma = math.sqrt(dataframe['squared_margin_to_mean'].sum()/(len(dataframe)-1))

    fig, ax = plt.subplots()
    dataframe.plot('id', 'ratio_vol_td', kind='scatter', ax=ax)

    sub_dataframe = pd.concat({'id': dataframe['id'], 'vol': dataframe['ratio_vol_td'], 'source': dataframe['group']}, axis=1)
    for i, point in islice(sub_dataframe.iterrows(), 0, 5):
        ax.text(point['id'], point['vol'], str(point['source']).split(",")[0].replace('(', ' '))

    plt.axhline(traffic_mean, color='r')
    # plt.axhline(sigma, color='g')
    plt.xlabel('IP ID')
    plt.ylabel('MB/s')

    plt.savefig("regression_analysis")
    dataframe.to_csv(save_csv_path, index=False)
