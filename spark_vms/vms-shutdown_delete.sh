#!/bin/bash
# ---------------------------------------------------------------------------------------------------------------------
#
#                                   	Bioinformatics Research Group
#										   http://biorg.cis.fiu.edu/
#                             		  Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this script is to stop and then remove any running virtual machines.
#
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#       • VirtualBox (VBoxManage)
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

SPARK_MASTER_NAME="Spark_Master"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Shutdow..."
VBoxManage controlvm $SPARK_MASTER_NAME poweroff

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Removing VMs..."
VBoxManage unregistervm $SPARK_MASTER_NAME -delete

echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""