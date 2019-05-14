import argparse
import os
import shutil

import DatasetGenerator
import Evaluator

HADOOP_PROJECT_MAIN = "/user/st-turetta/project"
HADOOP_PROJECT_PATH_INPUT = HADOOP_PROJECT_MAIN + "/input"
HADOOP_PROJECT_PATH_OUTPUT = HADOOP_PROJECT_MAIN + "/output"
PIG_SCRIPT_NAME ="udpfloodpcap.pig"
HEADER = "group,min_ts,max_ts,n_packets,total_volume,time_difference,ratio_vol_td"

def generation_routine(dataset_name, n_members, n_lines):
	print("Generating dataset: " + dataset_name + "...")
	DatasetGenerator.generate(dataset_name, int(n_members), int(n_lines))
	print("Copying dataset into hdfs:" + HADOOP_PROJECT_PATH_INPUT + "/" + dataset_name + "...")
	os.system("hadoop fs -put " + dataset_name + " " + HADOOP_PROJECT_PATH_INPUT)

def analysis_routine(dataset_name, save_csv_path):
	print("Analyzing " + dataset_name + "...")


	os.system("pig -x mapreduce -param filename=" + dataset_name + " " + PIG_SCRIPT_NAME )

	os.system("hadoop fs -copyToLocal " + HADOOP_PROJECT_PATH_OUTPUT + "/" + dataset_name)


	print("Copying and merging output..")
	with open(dataset_name + "_rawoutput_concat", 'a') as outfile:
		outfile.write(HEADER)
		for subdir, dirs, files in os.walk(dataset_name):
			for file in files:
				with open(file, 'r') as readfile:
					outfile.write("\n")
					shutil.copyfileobj(readfile, outfile)


	print("Clean up...")
	Evaluator.evaluate(dataset_name + "_rawoutput_concat",save_csv_path)
	os.remove(dataset_name + "_rawoutput_concat")
	os.rmdir(dataset_name)
	print("Done!")

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()
	group.add_argument("-g","--generate", help="Generate only dataset", nargs = 3, metavar = ('dataset_name', 'n_members', 'n_lines'))

	group.add_argument("-a", "--analyze", help="Analyze dataset with Pig and plot", nargs = 2, metavar = ('dataset_name', 'save_csv_path'))
	group.add_argument("-ga", "--genanalyze", help="Generate and analyze dataset with Pig and plot", nargs=4,
	                   metavar=('dataset_name', 'n_members', 'n_lines', 'save_csv_path'))
	args = parser.parse_args()

	if args.generate:
		generation_routine(args.generate[0],args.generate[1],args.generate[2])
	elif args.analyze:
		analysis_routine(args.analyze[0], args.analyze[1])
	elif args.genanalyze:
		generation_routine(args.genanalyze[0], args.genanalyze[1], args.genanalyze[2])
		analysis_routine(args.genanalyze[0], args.genanalyze[3])
	else:
		parser.print_help()







