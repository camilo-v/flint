#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                   		   Bioinformatics Research Group
#												http://biorg.cis.fiu.edu/
#                             			  	  Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this script is to copy required Bowtie2 indices into the worker nodes after an EMR deployment.
#   NOTE THAT THIS SCRIPT IS DEPRECATED.
#       - It was originally developed as a testing script for sequentially copying the index shards to each
#         worker node. It has value mostly for debugging purposes.
#
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#       • wget
#
#
#	AUTHOR:	Camilo Valdes (cvalde03@fiu.edu)
#			Bioinformatics Research Group,
#			School of Computing and Information Sciences,
#			Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------


APPS_DIR="/home/hadoop/apps"
BIODATA_DIR="/mnt/bio_data"
INDEX_DIR=$BIODATA_DIR"/index"

mkdir -p $BIODATA_DIR
mkdir -p $INDEX_DIR

CERT_NAME="your_spark_certificate.pem"
CERT_DIR="/home/hadoop/certs"
LOCAL_CERT=$CERT_DIR"/"$CERT_NAME

# ------------------------------------------------ Worker Nodes -------------------------------------------------------
#
#
WORKERS_LIST_FILE="/home/hadoop/workers_list.txt"

#	Get a list of the Worker nodes IP addresses
hadoop dfsadmin -report | grep Name: | cut -d' ' -f2 | cut -d':' -f1 > $WORKERS_LIST_FILE

#	Number of Worker nodes
NUMBER_WORKER_NODES=`wc -l < $WORKERS_LIST_FILE`

ENSEMBL_VERSION="41"
#	Copy the appropiate index shards into the Worker nodes
S3_INDEX_BASE_DIR="//s3-bucket-name/ensembl/"$ENSEMBL_VERSION"/partitions"

WORKER_COUNT=1
while read line
do
    printf "\n-------------------------------------------------------------\n"
    echo "Worker IP: $line, Index Partition: "$WORKER_COUNT
    WORKER_IP=$line

    SOURCE_DIR=s3:$S3_INDEX_BASE_DIR"/"$WORKER_COUNT
    echo "Source Dir: "
    echo $SOURCE_DIR


    TARGET_HOST="hadoop@$line"
    TARGET_DIR=$INDEX_DIR
    echo "Target Host"
    echo $TARGET_HOST
    echo $TARGET_DIR

    FLAG_FILE=$BIODATA_DIR'/index_copied.txt'

    EXISTS_COMMAND='if [ -f '$FLAG_FILE' ]; then echo yes; else echo no; fi'
    FILE_EXISTS=`ssh -n -i $LOCAL_CERT -o StrictHostKeyChecking=no $TARGET_HOST $EXISTS_COMMAND`
    echo "Flag Exists: "$FILE_EXISTS

    if [ "$FILE_EXISTS" == "no" ]; then

        echo "Index not found! Copying..."

        COPY_COMMAND='aws s3 cp --recursive '$SOURCE_DIR' '$TARGET_DIR
        echo "S3 Copy Command:"
        echo $COPY_COMMAND

        ssh -n -i $LOCAL_CERT -o StrictHostKeyChecking=no $TARGET_HOST $COPY_COMMAND

        # Flag that the index was already copied
        TOUCH_COMMAND='touch '$FLAG_FILE
        echo "Touch Flag Command "
        echo $TOUCH_COMMAND

        ssh -n -i $LOCAL_CERT -o StrictHostKeyChecking=no $TARGET_HOST $TOUCH_COMMAND

    fi

    ((WORKER_COUNT++))

    echo ""

done < $WORKERS_LIST_FILE

