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
#	The purpose of this script is to create the base image for a VM.
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
LOCAL_NETWORK_NAME=vboxnet0


# ------------------------------------------------------- Worker ------------------------------------------------------
#
#	Names should not contain any spaces!
#	Sizes are in Megabytes (MB)
#
SPARK_WORKER_NAME="Spark_Worker"
SPARK_WORKER_HD_NAME=$BASE_DIR"/images/spark_worker/Spark_Worker.vdi"
SPARK_WORKER_MEM_SIZE=16000
SPARK_WORKER_HD_SIZE=512000
SPARK_WORKER_VRDE_PORT=3391
SPARK_WORKER_CPUS=2

#
#	Deploy the Spark Worker
#
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Worker VM..."
VBoxManage createvm --name $SPARK_WORKER_NAME --register

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Modifying Worker VM..."
VBoxManage modifyvm $SPARK_WORKER_NAME --memory $SPARK_WORKER_MEM_SIZE \
					--acpi on \
					--boot1 dvd \
					--nictype1 Am79C973 \
					--nic1 nat \
					--nictype2 Am79C973 \
					--nic2 hostonly \
					--hostonlyadapter2 $LOCAL_NETWORK_NAME


echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Worker HD..."
VBoxManage createhd --filename $SPARK_WORKER_HD_NAME --size $SPARK_WORKER_HD_SIZE

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Worker Storage..."
VBoxManage storagectl $SPARK_WORKER_NAME --name "IDE Controller" --add ide

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Attaching Worker Storage..."
VBoxManage storageattach $SPARK_WORKER_NAME --storagectl "IDE Controller" --port 0 \
					--device 0 \
					--type hdd \
					--medium $SPARK_WORKER_HD_NAME

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Attaching Image to Worker Storage..."
VBoxManage storageattach $SPARK_WORKER_NAME --storagectl "IDE Controller" --port 1 \
					--device 0 \
					--type dvddrive\
					 --medium $UBUNTU_IMAGE

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Configuring CPUs..."
VBoxManage modifyvm $SPARK_WORKER_NAME --ioapic on --cpus $SPARK_WORKER_CPUS

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Remote Desktop services..."
VBoxManage modifyvm $SPARK_WORKER_NAME --vrde on

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up port forwarding..."
VBoxManage modifyvm $SPARK_WORKER_NAME --natpf1 "ssh,tcp,,3023,,22"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Deploying Worker VM..."
nohup VBoxHeadless --startvm $SPARK_WORKER_NAME --vrde on -p $SPARK_WORKER_VRDE_PORT &


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""


