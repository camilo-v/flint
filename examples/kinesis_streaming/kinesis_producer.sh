#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                  		Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this script is to show an example of how to start a Flint Kinesis stream producer.
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
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Example..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

BASE_DIR="/path/to/data/dir/sample"

#
#   Kinesis Stream particulars.
#
STREAM_NAME="flint_tab5_stream"
REGION_NAME="us-east-1"

#   The path of the reads we wish to place into the Kinesis stream.
TAB5_READS=${BASE_DIR}"/shards/paired-reads.txt"

#   Location of the 'flint_kinesis_producer.py' script.
SCRIPT_PATH="/path/to/flint/source"


#
#   Call the script.
#
${SCRIPT_PATH}/flint_kinesis_producer.py    --stream_name ${STREAM_NAME} \
                                            --region ${REGION_NAME} \
                                            --tab5_reads ${TAB5_READS}


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Example Finished."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo ""
