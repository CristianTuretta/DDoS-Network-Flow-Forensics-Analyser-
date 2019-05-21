import argparse
import glob
import os
import shutil
import PerformanceAnalyser as perfAnalyser
import DatasetGenerator
import Evaluator
import pwd

HADOOP_PROJECT_MAIN = "/user/" + pwd.getpwuid(os.getuid())[0] + "/project"
HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER = "/ratio_vol_td"
HADOOP_PROJECT_PATH_INPUT = HADOOP_PROJECT_MAIN + "/input"
HADOOP_PROJECT_PATH_OUTPUT = HADOOP_PROJECT_MAIN + "/output"
PIG_SCRIPT_NAME = "udpfloodpcap.pig"
HEADER = "group;min_ts;max_ts;n_packets;total_volume;time_difference;ratio_vol_td"
AVG_LINE_SIZE = 70


def generation_routine(dataset_name, n_members, n_lines, n_attackers, atk_volume, atk_duration):
	print("Generating dataset: " + dataset_name + "...")
	DatasetGenerator.generate(dataset_name, int(n_members), int(n_lines), int(n_attackers), int(atk_volume),
	                          int(atk_duration))
	print("Copying dataset into hdfs:" + HADOOP_PROJECT_PATH_INPUT + "/" + dataset_name + "...")
	os.system("hadoop fs -put " + dataset_name + " " + HADOOP_PROJECT_PATH_INPUT)


def analysis_routine(dataset_name, pig=True):
	output_path = "outputs/" + dataset_name + "/"
	folder2create = os.path.dirname(output_path)
	if not os.path.exists(folder2create):
		os.makedirs(folder2create)

	print("Analyzing " + dataset_name + "...")

	if pig:
		os.system("pig -x mapreduce -param filename=" + dataset_name + " " + PIG_SCRIPT_NAME)
	os.system(
		"hadoop fs -copyToLocal " + HADOOP_PROJECT_PATH_OUTPUT + "/" + dataset_name + HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER + " " + output_path)

	print("Copying and merging output..")
	with open(output_path + dataset_name + "_rawoutput_concat", 'w') as outfile:
		outfile.write(HEADER + "\n")
		for file in sorted(glob.glob(output_path + HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER[1:] + "/part-r-*")):
			with open(file, 'r') as readfile:
				shutil.copyfileobj(readfile, outfile)

	print("Evaluating...")
	Evaluator.evaluate(dataset_name=dataset_name, dataset_path=output_path + dataset_name + "_rawoutput_concat",
	                   output_path=output_path)

	print("Clean up...")
	os.remove(output_path + dataset_name + "_rawoutput_concat")
	shutil.rmtree(output_path + HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER[1:])
	print("Done!")


def sizeof_fmt(num, suffix='B'):
	for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
		if abs(num) < 1024.0:
			return "%3.1f%s%s" % (num, unit, suffix)
		num /= 1024.0
	return "%.1f%s%s" % (num, 'Yi', suffix)


def size_estimation_routine(records_length, n_attackers, atk_volume, atk_duration):
	return sizeof_fmt(AVG_LINE_SIZE * (int(records_length) + (int(atk_volume) / 1000) * int(atk_duration)))


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()
	group.add_argument("-g", "--generate", help="Generate only dataset",
	                   nargs=6,
	                   metavar=('dataset_name', 'n_members', 'n_lines', 'n_attackers', 'atk_volume', 'atk_duration'))

	group.add_argument("-a", "--analyze", help="Analyze dataset with Pig and plot",
	                   nargs=1, metavar=('dataset_name'))

	group.add_argument("-anp", "--analyzenopig", help="Analyze dataset already analyzed with Pig and plot",
	                   nargs=1, metavar=('dataset_name'))

	group.add_argument("-ga", "--genanalyze", help="Generate and analyze dataset with Pig and plot",
	                   nargs=6,
	                   metavar=('dataset_name', 'n_members', 'n_lines', 'n_attackers', 'atk_volume', 'atk_duration'))

	group.add_argument("-sga", "--simgenanalyze", help="Simulate, generate and analyze dataset with Pig and plot",
	                   nargs=6,
	                   metavar=('dataset_name', 'n_members', 'n_lines', 'n_attackers', 'atk_volume', 'atk_duration'))

	args = parser.parse_args()

	if args.generate:
		perfAnalyser.performance_eval(generation_routine, args.generate[0], args.generate[1], args.generate[2],
		                              args.generate[3], args.generate[4], args.generate[5])
	elif args.analyze:
		perfAnalyser.performance_eval(analysis_routine, args.analyze[0], True)
	elif args.analyzenopig:
		perfAnalyser.performance_eval(analysis_routine, args.analyzenopig[0], False)
	elif args.genanalyze:
		perfAnalyser.performance_eval(generation_routine, args.genanalyze[0], args.genanalyze[1], args.genanalyze[2],
		                              args.genanalyze[3], args.genanalyze[4], args.genanalyze[5])
		perfAnalyser.performance_eval(analysis_routine, args.genanalyze[0], True)
	elif args.simgenanalyze:
		print(size_estimation_routine(args.simgenanalyze[2],
		                              args.simgenanalyze[3], args.simgenanalyze[4], args.simgenanalyze[5]))
		choice = input("Would you like to continue? (Y/N)")

		if(choice == 'Y'):
			perfAnalyser.performance_eval(generation_routine, args.simgenanalyze[0], args.simgenanalyze[1],
			                              args.simgenanalyze[2],
			                              args.simgenanalyze[3], args.simgenanalyze[4], args.simgenanalyze[5])
			perfAnalyser.performance_eval(analysis_routine, args.simgenanalyze[0], True)
	else:
		parser.print_help()
