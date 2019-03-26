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
#	The purpose of this script is to start the Spark Master VM after it has been created.  The 'deploy' script creates
#	the VMs from scratch and launches them after, but this script just starts them after they have been created.
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
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Spark Master..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"


BASE_DIR="/path/to/base/directory/apps/virtualbox"


# ------------------------------------------------------- Master ------------------------------------------------------

SPARK_MASTER_NAME="Spark_Master"
SPARK_MASTER_VRDE_PORT=3390

nohup VBoxHeadless --startvm $SPARK_MASTER_NAME --vrde on -p $SPARK_MASTER_VRDE_PORT &


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""
