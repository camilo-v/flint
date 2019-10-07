#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                  		Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this script is to act as a basic driver of the index provisioning script.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#		• Apache-Spark
#       • Python
#		• flint.py
#
#	AUTHOR:
#			Camilo Valdes (camilo@castflyer.com)
#			Florida International University (FIU)
#
#
# ---------------------------------------------------------------------------------------------------------------------

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Test..."

#	Base location for the project
BASE_DIR='/mnt/bio_data'

PROJECT_DIR='/home/hadoop/flint'

#   The S3 path to were the partitioned index is located at.
BOWTIE2_INDEX_S3_LOCATION="s3://bucket-name/path/to/the/partitions_64"

NUMBER_OF_SHARDS="64"

# --------------------------------------------------- Spark Cluster ---------------------------------------------------
#
#   Cluster particulars (URL, executors, etc.) are configured here.  From the Spark documentation "Unlike Spark
#   standalone and Mesos modes, in which the master’s address is specified in the --master parameter, in YARN mode
#   the ResourceManager’s address is picked up from the Hadoop configuration. Thus, the --master parameter is 'yarn'."
#
URL_FOR_SPARK_CLUSTER="yarn"

#   Amount of memory to allocate for the driver process.
MEMORY_FOR_DRIVER="10G"

#   The number of cores that the Driver process will use.
NUMBER_OF_DRIVER_CORES="8"

#   The number of Executor processes to launch in each Worker node.
NUMBER_OF_EXECUTORS=${NUMBER_OF_SHARDS}

#   The number of cores that each Executor process will use.
NUMBER_OF_EXECUTOR_CORES="1"

#   Amount of memory to allocate for the executor processes.
MEMORY_FOR_EXECUTORS="10G"

#   The YARN queue in which the job will run on.
YARN_QUEUE="default"

DEPLOY_MODE="client"

KINESIS_LIB_PATH="/usr/lib/spark/external/lib/spark-streaming-kinesis-asl-assembly.jar"


#
#	Submit the script to the Spark cluster using "bin/spark-submit".
#
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

#
spark-submit    --jars $KINESIS_LIB_PATH \
                --master $URL_FOR_SPARK_CLUSTER \
                --deploy-mode $DEPLOY_MODE \
                --queue $YARN_QUEUE \
                --num-executors $NUMBER_OF_EXECUTORS\
                --executor-cores $NUMBER_OF_EXECUTOR_CORES\
                $SPARK_CONF \
                $PROJECT_DIR/provision_index.py --shards ${NUMBER_OF_SHARDS} \
                                                --bowtie2_index ${BOWTIE2_INDEX_S3_LOCATION} \
                                                --verbose


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Test Finished."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo ""
