import argparse
import glob
import os
import shutil

import DatasetGenerator
import Evaluator

HADOOP_PROJECT_MAIN = "/user/st-turetta/project"
HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER = "/ratio_vol_td"
HADOOP_PROJECT_PATH_INPUT = HADOOP_PROJECT_MAIN + "/input"
HADOOP_PROJECT_PATH_OUTPUT = HADOOP_PROJECT_MAIN + "/output"
PIG_SCRIPT_NAME ="udpfloodpcap.pig"
HEADER = "group;min_ts;max_ts;n_packets;total_volume;time_difference;ratio_vol_td"

def generation_routine(dataset_name, n_members, n_lines):
	print("Generating dataset: " + dataset_name + "...")
	DatasetGenerator.generate(dataset_name, int(n_members), int(n_lines))
	print("Copying dataset into hdfs:" + HADOOP_PROJECT_PATH_INPUT + "/" + dataset_name + "...")
	os.system("hadoop fs -put " + dataset_name + " " + HADOOP_PROJECT_PATH_INPUT)

def analysis_routine(dataset_name):

	output_path = "outputs/" + dataset_name + "/"
	folder2create = os.path.dirname(output_path)
	if not os.path.exists(folder2create):
		os.makedirs(folder2create)

	print("Analyzing " + dataset_name + "...")


	os.system("pig -x mapreduce -param filename=" + dataset_name + " " + PIG_SCRIPT_NAME )

	os.system("hadoop fs -copyToLocal " + HADOOP_PROJECT_PATH_OUTPUT + "/" + dataset_name + HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER + " " + output_path)


	print("Copying and merging output..")
	with open(output_path + dataset_name + "_rawoutput_concat", 'w') as outfile:
		outfile.write(HEADER + "\n")
		for file in sorted(glob.glob(output_path + HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER[1:] + "/part-r-*")):
			with open(file, 'r') as readfile:
				shutil.copyfileobj(readfile, outfile)


	print("Evaluating...")
	Evaluator.evaluate(dataset_name=dataset_name, dataset_path= output_path + dataset_name + "_rawoutput_concat", output_path=output_path)

	print("Clean up...")
	os.remove(output_path + dataset_name + "_rawoutput_concat")
	shutil.rmtree(output_path + HADOOP_PROJECT_PATH_OUTPUT_SUBFOLDER[1:])
	print("Done!")

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()
	group.add_argument("-g","--generate", help="Generate only dataset", nargs = 3, metavar = ('dataset_name', 'n_members', 'n_lines'))

	group.add_argument("-a", "--analyze", help="Analyze dataset with Pig and plot", nargs = 1, metavar = ('dataset_name'))
	group.add_argument("-ga", "--genanalyze", help="Generate and analyze dataset with Pig and plot", nargs=3,
	                   metavar=('dataset_name', 'n_members', 'n_lines'))
	args = parser.parse_args()

	if args.generate:
		generation_routine(args.generate[0],args.generate[1],args.generate[2])
	elif args.analyze:
		analysis_routine(args.analyze[0])
	elif args.genanalyze:
		generation_routine(args.genanalyze[0], args.genanalyze[1], args.genanalyze[2])
		analysis_routine(args.genanalyze[0])
	else:
		parser.print_help()







