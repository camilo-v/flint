#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                  		Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this script is to show an example of how to run a Flint analysis job.
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

PROJECT_DIR='/path/to/project/dir/flint'

#	File with Samples we want to process.  The format of this file is JSON.
CONF_SAMPLES_JSON=$PROJECT_DIR'/examples/directory_streaming/configurations/streaming_configuration-s3_output.json'


# --------------------------------------------------- Spark Cluster ---------------------------------------------------
#
#   Cluster particulars (URL, executors, etc.) are configured here.  From the Spark documentation "Unlike Spark
#   standalone and Mesos modes, in which the master’s address is specified in the --master parameter, in YARN mode
#   the ResourceManager’s address is picked up from the Hadoop configuration. Thus, the --master parameter is 'yarn'."
#
URL_FOR_SPARK_CLUSTER="yarn"

YARN_QUEUE="default"

DEPLOY_MODE="client"

KINESIS_LIB_PATH="/usr/lib/spark/external/lib/spark-streaming-kinesis-asl-assembly.jar"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

#
#	Submit the script to the cluster using "bin/spark-submit".
#
#
spark-submit    --jars ${KINESIS_LIB_PATH} \
                --master ${URL_FOR_SPARK_CLUSTER} \
                --deploy-mode ${DEPLOY_MODE} \
                --queue ${YARN_QUEUE} \
                ${PROJECT_DIR}/flint.py --samples ${CONF_SAMPLES_JSON} \
					                    --output_s3 \
					                    --report_all \
					                    --coalesce_output \
					                    --stream_dir


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Example Finished."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo ""
