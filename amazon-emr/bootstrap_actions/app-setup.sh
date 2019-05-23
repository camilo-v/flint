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
#	The purpose of this script is to copy required software into the worker nodes during an EMR deployment.
#	This script is called when an EMR cluster is provisioned during the "Additional Options" and "Bootstrap Actions".
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


#	Exit immediately if a command exits with a non-zero exit status.
set -e

#	Default Data and Application directories in the worker nodes
APPS_DIR="/home/hadoop/apps"
BIODATA_DIR="/mnt/bio_data"
INDEX_DIR=$BIODATA_DIR"/index"

mkdir -p $APPS_DIR
mkdir -p $BIODATA_DIR
mkdir -p $INDEX_DIR

#	Versions of the tools we'll be installing
BOWTIE_VERSION="2.3.4.1"
SAMTOOLS_VERSION="1.3.1"

SOURCE_BUCKET_NAME="s3-bucket-name"

#	Bowtie
wget https://$SOURCE_BUCKET_NAME.s3.amazonaws.com/apps/bowtie2-$BOWTIE_VERSION-linux-x86_64.zip
unzip -d $APPS_DIR bowtie2-$BOWTIE_VERSION-linux-x86_64.zip
BOWTIE_DIR=$APPS_DIR"/bowtie2-"$BOWTIE_VERSION"-linux-x86_64"

cd ~

# ---------------------------------------------- Environment Setup ----------------------------------------------------
#
#	Setup our bash_profile
echo "" >> ~/.bash_profile
echo "alias l='ls -lhF'" >> ~/.bash_profile

#	Add bowtie2 to the path
echo "" >> ~/.bashrc
printf '\n\n# Bowtie2\n' >> ~/.bashrc
printf 'PATH=$PATH:'$BOWTIE_DIR'/; export PATH\n' >> ~/.bashrc

# -------------------------------------------------- Cert Keys --------------------------------------------------------
#
#	The certificate is used so we can login into the worker nodes.
#
CERT_NAME="your_spark_certificate.pem"
S3_CERT="//"$SOURCE_BUCKET_NAME"/certs/"$CERT_NAME

CERT_DIR="/home/hadoop/certs"
mkdir -p $CERT_DIR

aws s3 cp s3:$S3_CERT $CERT_DIR

LOCAL_CERT=$CERT_DIR"/"$CERT_NAME
chmod 400 $LOCAL_CERT


# ------------------------------------------------- Worker List -------------------------------------------------------
#
#	Copy the script that retrieves the Worker node ip addresses. Used for debugging.
#
WORKER_LIST_SCRIPT_NAME="get_worker_ip_list.sh"
S3_WORKER_LIST_SCRIPT_PATH="//"$SOURCE_BUCKET_NAME"/steps/"$WORKER_LIST_SCRIPT_NAME

aws s3 cp s3:$S3_WORKER_LIST_SCRIPT_PATH $SCRIPTS_DIR

LOCAL_WORKER_LIST_SCRIPT=$SCRIPTS_DIR"/"$WORKER_LIST_SCRIPT_NAME
chmod ug+rwx $LOCAL_WORKER_LIST_SCRIPT


# ---------------------------------------------- Flint Project Dir ----------------------------------------------------
#
#	As a convenience, we'll create the directory were we'll be droping the source code.
#
FLINT_HOME="/home/hadoop/flint"
mkdir -p $FLINT_HOME


# ------------------------------------------------ Python Libs --------------------------------------------------------
#
#   Some Python libraries are not pre-installed. So we have to install them as part of the Cluster provisioning step.
#
sudo pip install pathlib2
sudo pip install biopython


# ------------------------------------------------ Spark Conf ---------------------------------------------------------
#
#   Copy the custom Spark configurations.
#
CUSTOM_CONFS="/home/hadoop/confs"
mkdir -p $CUSTOM_CONFS

S3_CUSTOM_CONFS="//"$SOURCE_BUCKET_NAME"/config_files"

LOG4J_NAME="log4j.properties"
LOG4J_CONF=$S3_CUSTOM_CONFS"/"$LOG4J_NAME

SPARK_CONF_DIR="/etc/spark/conf"

#   Copy the conf from S3 into the local filesystem
aws s3 cp s3:$LOG4J_CONF $CUSTOM_CONFS



