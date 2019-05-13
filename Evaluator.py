import pandas as pd
import matplotlib.pyplot as plt
import sys

if __name__ == '__main__':
    dataset_path = sys.argv[1]
    save_csv_path = sys.argv[2]

    dataframe = pd.DataFrame(pd.read_csv(dataset_path, sep=";"))
    elapsed_time = dataframe['time_difference'].sum()
    total_data_income = dataframe['total_volume'].sum()
    traffic_mean = total_data_income/elapsed_time

    dataframe.insert(loc=0, column='id', value=range(1, len(dataframe) + 1))

    dataframe['margin_to_mean'] = dataframe['total_volume']
    dataframe['margin_to_mean'] = dataframe['margin_to_mean'].apply(lambda x: x - traffic_mean)

    dataframe.plot(kind="scatter", x="id", y="ratio_vol_td", color="black")
    plt.axhline(traffic_mean)
    plt.xlabel('IP ID')
    plt.ylabel('MB/s')

    plt.savefig("regression_analysis")
    dataframe.to_csv(save_csv_path, index=False)
