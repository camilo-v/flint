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
#	The purpose of this script is to increase the storage capacity of a virtual machine Spark worker node.
#
#   NOTES:
#   The virtual box command to use is 'modifyhd' but since we are using snapshots and 'copy-on-write', we need to
#	modify the hdds snapshots themselves (the latest ones) and not the base .VDI we use.
#
#
#   DEPENDENCIES:
#
#       • VBoxManage
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
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"


SPARK_WORKER_HD_SIZE=512000

VBoxManage modifyhd "/path/to/VirtualBox/VMs/Spark_Worker_1/Snapshots/{37e2a7f7-b108-42ba-81da-756a05378b61}.vdi" --resize $SPARK_WORKER_HD_SIZE
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

VBoxManage modifyhd "/path/to/VirtualBox/VMs/Spark_Worker_2/Snapshots/{354e934f-894b-4da9-a09d-cc53f64ea946}.vdi" --resize $SPARK_WORKER_HD_SIZE
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

VBoxManage modifyhd "/path/to/VirtualBox/VMs/Spark_Worker_3/Snapshots/{152d3dab-8d8f-49a5-8040-e65c174c16da}.vdi" --resize $SPARK_WORKER_HD_SIZE
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

VBoxManage modifyhd "/path/to/VirtualBox/VMs/Spark_Worker_4/Snapshots/{e8af5de1-1860-4eb6-b4a9-c77fbe4c750a}.vdi" --resize $SPARK_WORKER_HD_SIZE
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""