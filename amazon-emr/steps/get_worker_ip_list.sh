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
#	The purpose of this script is to get a list of the Worker Nodes in 'this' cluster. Specifically, we want to
#   obtain the private DNS names for each worker node so that we may debug them.
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

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Analysis..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

WORKERS_LIST_FILE="/home/hadoop/workers_list.txt"

#	Get a list of the Worker nodes IP addresses
hadoop dfsadmin -report | grep Name: | cut -d' ' -f2 | cut -d':' -f1 > $WORKERS_LIST_FILE

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Worker Nodes list ready at: "$WORKERS_LIST_FILE
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

#	Number of Worker nodes
NUMBER_WORKER_NODES=`wc -l < $WORKERS_LIST_FILE`

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Number of Worker Nodes: "$NUMBER_WORKER_NODES
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
