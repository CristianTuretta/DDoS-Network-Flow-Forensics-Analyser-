import cProfile
import pstats
import datetime
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from itertools import islice
import os

HEADER = 'time,kind,name,seconds'

def performance_eval(function, *args):
	history_file = open("PerformanceHistory.csv", "a+")

	if not os.path.getsize("PerformanceHistory.csv") > 0:
		history_file.write(HEADER + "\n")


	profiler = cProfile.Profile()
	profiler.enable()
	function(*args)
	profiler.disable()

	stats = pstats.Stats(profiler)

	if function.__name__ == 'generation_routine':
		# generation branch
		history_file.write(
			str(datetime.datetime.now().time()).split('.')[0] + ",g," + args[0] + "," + str(stats.total_tt) + "\n")
	elif function.__name__ == 'analysis_routine':
		# analysis branch
		history_file.write(
			str(datetime.datetime.now().time()).split('.')[0] + ",a," + args[0] + "," + str(stats.total_tt) + "\n")
	history_file.close()


def plot_stats(stats_df):
	sub_dataframe = pd.concat(
		{'id': stats_df['id'], 'name': stats_df['name'], 'minutes': stats_df['seconds']/60}, axis=1)

	sub_dataframe.plot('id', 'minutes', kind='scatter', ax=ax)

	for i, point in islice(sub_dataframe.iterrows(), 0, 5):
		ax.text(point['id'], point['minutes'], str(point['name']).split(".")[0])
	plt.savefig(args.analysis_sts[0], dpi=300)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()

	group.add_argument("-a", "--analysis_sts", help="Plot Analysis Stats", nargs=1, metavar='img_name')
	group.add_argument("-g", "--generation_sts", help="Plot Dataset Generation Stats", nargs=1, metavar='img_name')

	args = parser.parse_args()

	dataframe = pd.DataFrame(pd.read_csv("PerformanceHistory.csv", sep=","))
	fig, ax = plt.subplots()

	if args.analysis_sts:
		analysis_df = dataframe[dataframe['kind'] == 'a']
		analysis_df.insert(loc=0, column='id', value=range(1, len(analysis_df) + 1))
		plot_stats(analysis_df)
	elif args.generation_sts:
		generation_df = dataframe[dataframe['kind'] == 'g']
		generation_df.insert(loc=0, column='id', value=range(1, len(generation_df) + 1))
		plot_stats(generation_df)
	else:
		parser.print_help()