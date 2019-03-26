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
#   OBJECTIVE: (DEPRECATED)
#	The purpose of this script is to copy the required genome annotations into the Master node.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#       • aws cli
#
#
#	AUTHOR:	Camilo Valdes (cvalde03@fiu.edu)
#			Bioinformatics Research Group,
#			School of Computing and Information Sciences,
#			Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Copy..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

#	Default Data and Application directories in the worker nodes
APPS_DIR="/home/hadoop/apps"
BIODATA_DIR="/mnt/bio_data"

mkdir -p $APPS_DIR
mkdir -p $BIODATA_DIR

# ------------------------------------------- Annotations (DEPRECATED) ------------------------------------------------
#
#	Copy the annotations (genome sequence reference names) into the cluster as a convenience so we don't have
#	to copy them later on.
#
ENSEMBL_VERSION="41"
ANNOTATIONS_FILE_NAME="example_annotations.txt"
S3_ANNOTATIONS="//s3-bucket-name/ensembl/"$ENSEMBL_VERSION"/annotations/"$ANNOTATIONS_FILE_NAME

ANNOTATIONS_DIR=$BIODATA_DIR"/annotations"
mkdir -p $ANNOTATIONS_DIR

aws s3 cp s3:$S3_ANNOTATIONS $ANNOTATIONS_DIR

echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
