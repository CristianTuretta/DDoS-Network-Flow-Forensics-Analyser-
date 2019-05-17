import math
from itertools import islice
import pandas as pd
import matplotlib.pyplot as plt


def evaluate(dataset_name, dataset_path, output_path):
    dataframe = pd.DataFrame(pd.read_csv(dataset_path, sep=";"))
    elapsed_time = dataframe['time_difference'].sum()
    total_data_income = (dataframe['total_volume'].sum())/(1**6) #Mb
    traffic_mean_vol = total_data_income/elapsed_time
    traffic_mean = total_data_income/len(dataframe)
    dataframe['total_volume'] /= (1**6)

    dataframe.insert(loc=0, column='id', value=range(1, len(dataframe) + 1))

    dataframe['margin_to_mean'] = dataframe['total_volume']
    dataframe['margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: x - traffic_mean_vol)

    dataframe['squared_margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: (x - traffic_mean_vol)**2)
    sigma = math.sqrt(dataframe['squared_margin_to_mean'].sum()/(len(dataframe)-1))

    fig, ax = plt.subplots()
    dataframe.plot('id', 'ratio_vol_td', kind='scatter', ax=ax)

    sub_dataframe = pd.concat({'id': dataframe['id'], 'vol': dataframe['ratio_vol_td'], 'source': dataframe['group']},
                              axis=1)
    for i, point in islice(sub_dataframe.iterrows(), 0, 5):
        ax.text(point['id'], point['vol'], str(point['source']).split(",")[0].replace('(', ' '))

    plt.axhline(traffic_mean_vol, color='g')
    plt.xlabel('IP ID')
    plt.ylabel('Mb/s')

    plt.savefig(output_path + dataset_name + "-diff_from_mean_analysis.png", dpi=300)

    fig, ax = plt.subplots()
    dataframe.plot('id', 'squared_margin_to_mean', kind='scatter', ax=ax)

    sub_dataframe = pd.concat({'id': dataframe['id'], 'mtm': dataframe['squared_margin_to_mean'],
                               'source': dataframe['group']}, axis=1)
    for i, point in islice(sub_dataframe.iterrows(), 0, 5):
        ax.text(point['id'], point['mtm'], str(point['source']).split(",")[0].replace('(', ' '))

    plt.axhline(sigma**2, color='r')
    plt.xlabel('IP ID')
    plt.ylabel('Squared margin to mean')

    plt.savefig(output_path + dataset_name + "-squared_margin_to_mean_analysis.png", dpi=300)

    dataframe.to_csv(output_path + dataset_name + "-indexed", index=False)
