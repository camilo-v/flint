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
#	The purpose of this script is to deploy a set of Virtual Machines (VMs) that will collectively act as a Spark
#	cluster.  The overall idea is to deploy an odd number of nodes that collectively form the Spark cluster. The
#	reason for the odd number is that one node will be the 'Master' node, and the remaining n-1 nodes will be the
#	workers (Spark Executors).
#
#	This script launches onde VM that will act as the Master node.
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
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"


BASE_DIR="/path/to/base/directory/apps/virtualbox"


# ------------------------------------------------------- Images ------------------------------------------------------
#
#	Base image template for the VMs.
#
UBUNTU_IMAGE=$BASE_DIR"/images/ubuntu-14.04.5-server-amd64.iso"


# ----------------------------------------------------- Networking ----------------------------------------------------
#
#	Setup the 'localhost' network that the nodes will use to communicate
#
LOCAL_NETWORK_IP=192.168.56.1
LOCAL_NETWORK_NAME=vboxnet0

vboxmanage hostonlyif create
vboxmanage hostonlyif ipconfig $LOCAL_NETWORK_NAME --ip $LOCAL_NETWORK_IP

# ------------------------------------------------------- Master ------------------------------------------------------
#
#	Names should not contain any spaces!
#	Sizes are in Megabytes (MB)
#
SPARK_MASTER_NAME="Spark_Master"
SPARK_MASTER_HD_NAME=$BASE_DIR"/images/spark_master/Spark_Master.vdi"
SPARK_MASTER_MEM_SIZE=16000
SPARK_MASTER_HD_SIZE=512000
SPARK_MASTER_VRDE_PORT=3390
SPARK_MASTER_CPUS=2

#
#	Deploy the Spark Master
#
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Master VM..."
VBoxManage createvm --name $SPARK_MASTER_NAME --register

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Modifying Master VM..."
VBoxManage modifyvm $SPARK_MASTER_NAME --memory $SPARK_MASTER_MEM_SIZE \
					--acpi on \
					--boot1 dvd \
					--nictype1 Am79C973 \
					--nic1 nat \
					--nictype2 Am79C973 \
					--nic2 hostonly \
					--hostonlyadapter2 $LOCAL_NETWORK_NAME


echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Master HD..."
VBoxManage createhd --filename $SPARK_MASTER_HD_NAME --size $SPARK_MASTER_HD_SIZE

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Master Storage..."
VBoxManage storagectl $SPARK_MASTER_NAME --name "IDE Controller" --add ide

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Attaching Master Storage..."
VBoxManage storageattach $SPARK_MASTER_NAME --storagectl "IDE Controller" --port 0 \
					--device 0 \
					--type hdd \
					--medium $SPARK_MASTER_HD_NAME

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Attaching Image to Master Storage..."
VBoxManage storageattach $SPARK_MASTER_NAME --storagectl "IDE Controller" --port 1 \
					--device 0 \
					--type dvddrive\
					 --medium $UBUNTU_IMAGE

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Configuring CPUs..."
VBoxManage modifyvm $SPARK_MASTER_NAME --ioapic on --cpus $SPARK_MASTER_CPUS

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Remote Desktop services..."
VBoxManage modifyvm $SPARK_MASTER_NAME --vrde on

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up SSH port forwarding..."
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "ssh,tcp,,3022,,22"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up Hadoop port forwarding..."
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "namenode,tcp,,50070,,50070"
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "namenode2,tcp,,8088,,8088"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up Ganglia port forwarding..."
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "ganglia,tcp,,8649,,8649"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up Apache port forwarding..."
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "apache,tcp,,80,,80"
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "apache2,tcp,,443,,443"
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "apache3,tcp,,8888,,8888"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up Spark port forwarding..."
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "spark,tcp,,8080,,8080"
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "spark2,tcp,,4040,,4040"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up VRDE port forwarding..."
VBoxManage modifyvm $SPARK_MASTER_NAME --natpf1 "vrde,tcp,,"$SPARK_MASTER_VRDE_PORT",,"$SPARK_MASTER_VRDE_PORT""

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Deploying Master VM..."
nohup VBoxHeadless --startvm $SPARK_MASTER_NAME --vrde on -p $SPARK_MASTER_VRDE_PORT &



echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""
