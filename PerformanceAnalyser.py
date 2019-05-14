import cProfile
import pstats
import datetime
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from itertools import islice


def performance_eval(function, *args):
	history_file = open("PerformanceHistory.csv", "a+")

	profiler = cProfile.Profile()
	profiler.enable()
	function(*args)
	profiler.disable()

	stats = pstats.Stats(profiler)

	if len(args) == 3:
		# generation branch
		history_file.write(
			str(datetime.datetime.now().time()).split('.')[0] + ",g," + args[0] + "," + str(stats.total_tt) + "\n")
	elif len(args) == 2:
		# analysis branch
		history_file.write(
			str(datetime.datetime.now().time()).split('.')[0] + ",a," + args[0] + "," + str(stats.total_tt) + "\n")

	history_file.close()


def plot_stats(stats_df):
	sub_dataframe = pd.concat(
		{'id': stats_df['id'], 'name': stats_df['name'], 'seconds': stats_df['seconds']}, axis=1)

	sub_dataframe.plot('id', 'seconds', kind='scatter', ax=ax)
	for i, point in islice(sub_dataframe.iterrows(), 0, 5):
		ax.text(point['id'], point['seconds'], str(point['name']).split(".")[0])

	plt.savefig(args.analysis_sts[0])


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()

	group.add_argument("-a", "--analysis_sts", help="Plot Analysis Stats", nargs=1, metavar='img_name')
	group.add_argument("-g", "--generation_sts", help="Plot Dataset Generation Stats", nargs=1, metavar='img_name')

	args = parser.parse_args()

	dataframe = pd.DataFrame(pd.read_csv("PerformanceHistory.csv", sep=","))
	dataframe.insert(loc=0, column='id', value=range(1, len(dataframe) + 1))

	fig, ax = plt.subplots()

	if args.analysis_sts:
		plot_stats(dataframe[dataframe['kind'] == 'a'])

	elif args.generation_sts:
		plot_stats(dataframe[dataframe['kind'] == 'g'])
