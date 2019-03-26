#!/usr/bin/python
# coding: utf-8

# ---------------------------------------------------------------------------------------------------------------------
#
#                                       Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	    The purpose of this program is to copy the Bowtie2 index to all the worker nodes in the cluster. The goal
#       is to copy the index in parallel, so that we don't copy the index sequentially and wait a lot of time.
#
#
#   NOTES:
#   Please see the dependencies section below for the required libraries (if any).
#
#   DEPENDENCIES:
#       • Apache-Spark
#       • Python
#       • R
#
#   You can check the python modules currently installed in your system by running: python -c "help('modules')"
#
#   USAGE:
#       Run the program with the "--help" flag to see usage instructions.
#
#	AUTHOR:
#           Camilo Valdes
#           cvalde03@fiu.edu
#           https://github.com/camilo-v
#			Florida International University, FIU
#           School of Computing and Information Sciences
#           Bioinformatics Research Group, BioRG
#           http://biorg.cs.fiu.edu/
#
#
# ---------------------------------------------------------------------------------------------------------------------

#   Spark Modules
from pyspark import SparkConf, SparkContext

# 	Python Modules
import sys
import argparse
import time
import collections
from datetime import timedelta
import subprocess as sp
import shlex

# -------------------------------------------------------- Main -------------------------------------------------------
#
#
def main(args):
    """
    Main function of the app.
    Args:
        args: command line arguments.

    Returns:

    """

    #
    #   Command-line Arguments, and miscellaneous declarations.
    #
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards", required=True, type=int, help="Number of Bowtie2 Index Shards.")
    args = parser.parse_args(args)

    index_location_s3 = "s3://your-bucket-name/ensembl/41/indices/partitions_64"
    number_of_index_shards = args.shards

    #
    #   Spark Configuration, and App declaration.
    #
    APP_NAME = "Index_Provisioning"
    conf = (SparkConf().setAppName(APP_NAME))

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Configuring Spark...")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")

    #   Initialize the Spark context for this run.
    sc = SparkContext(conf=conf)

    start_time = time.time()

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Starting Index Copy from: ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] " + index_location_s3)
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")

    #   Prepare the locations that will be copied.
    list_of_index_shards = []
    range_end = number_of_index_shards + 1  # Range function does not include the end.
    for partition_id in range(1, range_end):
        s3_location = index_location_s3 + "/" + str(partition_id)
        list_of_index_shards.append(s3_location)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Index Shards to Copy:")

    for location in list_of_index_shards:
        print(location)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")

    index_shards = sc.parallelize(list_of_index_shards)
    # index_shards = index_shards.repartition(number_of_index_shards)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] No. RDD Partitions: " +
          str(index_shards.getNumPartitions()))

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Starting to copy...")

    #
    #   The acknowledge_RDD will dispatch the "copy_index_to_worker()" function to all the workers via the
    #   "mapPartitions()" function.
    #
    acknowledge_RDD = index_shards.mapPartitions(copy_index_to_worker)

    #
    #   Once the "mapPartitions()" has executed (not really because it lazely evaluated), we'll collect the
    #   responses from all the nodes.
    #
    acknowledge_list = acknowledge_RDD.collect()
    acknowledge_list.sort()

    number_of_acknowledgements = len(acknowledge_list)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] No. of Workers that signaled: " +
          str(number_of_acknowledgements))
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")

    for ack in acknowledge_list:
        print(ack)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Duplicate Worker IPs:")

    print [item for item, count in collections.Counter(acknowledge_list).items() if count > 1]


    end_time = time.time()
    run_time = end_time - start_time

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Index Copy Time: " +
          "" + str(timedelta(seconds=run_time)) + "")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Done.")

    # ----------------------------------------------- Spark Stop --------------------------------------------------
    #
    #   Shut down the cluster once everything completes.
    sc.stop()


def copy_index_to_worker(iterator):
    """
    Function that runs on all the Worker nodes. It will copy the Index from the specified location into the worker's
    local filesystem.
    Args:
        iterator:

    Returns:

    """
    return_data = []

    worker_node_ip = str(sp.check_output(["hostname"])).strip()

    local_index_path = "/mnt/bio_data/index"

    for s3_location in iterator:

        aws_copy_command = "aws s3 cp --recursive " + s3_location + " " + local_index_path

        aws_process = sp.Popen(shlex.split(aws_copy_command), stdout=sp.PIPE, stderr=sp.PIPE)
        stdout, stderr = aws_process.communicate()
        return_data.append(worker_node_ip + "\n" +
                           "STDERR: " + stderr + "**********\n" +
                           "AWS COMMAND:\n" + aws_copy_command + "\n")

    return iter(return_data)



# ----------------------------------------------------------- Init ----------------------------------------------------
#
#   App Initializer.
#
if __name__ == "__main__":
    main(sys.argv[1:])



# -------------------------------------------------------- End of Line ------------------------------------------------
