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
#	This script launches a series of VMs that will act as the Worker nodes.  Note that all this script does is copies
#	existing Master node VM configuration and replicates it so that we do not have to spend much time installing and
#	configuring the base Ubuntu installation — it was previously done when we setup the Master.
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

#	We are using a single vm image as our base for the Spark Worker nodes, and VirtualBox will do copy-on-write
#	when each worker image is modified.
#	Refer to https://www.electricmonk.nl/log/2011/09/24/multiple-virtualbox-vms-using-one-base-image-copy-on-write
#	for a guide on the basic scheme of provisioning the vdi images as a base.
VDI_FILE="/path/to/base/directory/apps/virtualbox/clone_hds/spark-worker-base.vdi"


# ------------------------------------------------------- Workers -----------------------------------------------------
#
#	VM Names should not contain any spaces!
#	Sizes are in Megabytes (MB)
#
NUMBER_OF_WORKERS=4
SPARK_WORKER_VRDE_PORT=3391	#	The port number of the Worker Template is used as the base, and will be incremented.
SPARK_WORKER_SSH_PORT=3023	#	The ssh port number of the Master is also used as the base SSH port.
SPARK_WORKER_HD_SIZE=512000
SPARK_WORKER_MEM_SIZE=16000
SPARK_WORKER_CPUS=2

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Deploying $NUMBER_OF_WORKERS Workers:"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"


# ----------------------------------------------------- Networking ----------------------------------------------------
#
#	Setup the 'localhost' network that the nodes will use to communicate
#
LOCAL_NETWORK_NAME=vboxnet0


# ----------------------------------------------------- Provision VMs -------------------------------------------------
#
#	The first loop will setup and provision the VMs, but not launch them.  There is a bug in VirtualBox that when
#	'storageattach' is called with the '--multiattach' flag the VMs have to be powered down.
#
for WORKER_NO in $(seq 1 $NUMBER_OF_WORKERS)
{
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up worker No. "$WORKER_NO

	SPARK_WORKER="Spark_Worker_"$WORKER_NO

	SPARK_WORKER_SSH_PORT=$((SPARK_WORKER_SSH_PORT+1))
	NATP_FORWARD_STRING="ssh,tcp,,"$SPARK_WORKER_SSH_PORT",,22"


	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Worker VM..."
	VBoxManage createvm --name $SPARK_WORKER --register

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Modifying Worker Networking.."
	VBoxManage modifyvm $SPARK_WORKER --memory $SPARK_WORKER_MEM_SIZE \
					--acpi on \
					--boot1 dvd \
					--nictype1 Am79C973 \
					--nic1 nat \
					--nictype2 Am79C973 \
					--nic2 hostonly \
					--hostonlyadapter2 $LOCAL_NETWORK_NAME


	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Worker Storage Device..."
	VBoxManage storagectl $SPARK_WORKER --name "IDE Controller" --add ide

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Attaching Worker Storage..."

	if [ "$WORKER_NO" -gt 1 ]; then
		echo "[" `date '+%m/%d/%y %H:%M:%S'` "]   Adjusting storage registration string."
		# VDI_FILE=base.vdi
	fi

	VBoxManage storageattach $SPARK_WORKER --storagectl "IDE Controller" --port 0 \
						--device 0 \
						--type hdd \
						--medium $VDI_FILE \
						--mtype multiattach

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Configuring CPUs..."
	VBoxManage modifyvm $SPARK_WORKER --ioapic on --cpus $SPARK_WORKER_CPUS

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Remote Desktop services..."
	VBoxManage modifyvm $SPARK_WORKER --vrde on

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up port forwarding..."
	VBoxManage modifyvm $SPARK_WORKER --natpf1 $NATP_FORWARD_STRING

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] SSH Port Number: "$SPARK_WORKER_SSH_PORT"("$NATP_FORWARD_STRING")"
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
}

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting VMs..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"



# ------------------------------------------------- Start VMs ---------------------------------------------------------
#
#	After the VMs have been provisioned we can start them
#	Second loop will start the VMs after they have been provisioned.
#

SPARK_WORKER_VRDE_PORT=3391	# Reset the starting range.

for WORKER_NO in $(seq 1 $NUMBER_OF_WORKERS)
{
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Launching VM Worker \""$SPARK_WORKER"\" ($WORKER_NO)"

	SPARK_WORKER="Spark_Worker_"$WORKER_NO
	SPARK_WORKER_VRDE_PORT=$((SPARK_WORKER_VRDE_PORT+1))

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Setting up VRDE port forwarding..."
	VBoxManage modifyvm $SPARK_WORKER --natpf1 "vrde,tcp,,"$SPARK_WORKER_VRDE_PORT",,"$SPARK_WORKER_VRDE_PORT""

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Worker VM..."
	nohup VBoxHeadless --startvm $SPARK_WORKER --vrde on -p $SPARK_WORKER_VRDE_PORT &

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] VRDE Port Number: "$SPARK_WORKER_VRDE_PORT
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

}



echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""
