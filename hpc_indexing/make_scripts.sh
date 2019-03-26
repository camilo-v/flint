#!/bin/sh

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
#	The purpose of this script is to create the necessary partition indexing scripts for the High Performance
#	Cluster.  The Cluster runs the LSF scheduler, so the scripts created will have the necessary bsub submission
#	headers.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#       • bash
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
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Script Factory..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

# --------------------------------------------- Shell iVars -----------------------------------------------

BASE_DIR='/path/to/directory/for/hpc_indexing'

OUTPUT_PATH=$BASE_DIR'/hpc_indexing_scripts'
mkdir -p $OUTPUT_PATH

TEMPLATE_SCRIPT_PATH=$BASE_DIR'/bowtie2-hpc-indexing-template.sh'


# ------------------------------------------- Script Creation ---------------------------------------------


ARRAY_OF_PARTITIONS=( '48' '64' '128' '256' '512' )

for PARTITION in "${ARRAY_OF_PARTITIONS[@]}"
{

	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
    echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating Scripts for "$PARTITION" ..."

    PARTITION_OUTPUT_DIR=$OUTPUT_PATH"/partition_"$PARTITION
    mkdir -p $PARTITION_OUTPUT_DIR

    # -------------------------------------------- Partitions --------------------------------------------
    #
    #	Inner loop creates the 'n' necessary indexing scripts
    #
    NUMBER_OF_PARTITIONS=$PARTITION

	for i in `seq 1 $NUMBER_OF_PARTITIONS`;
	do
	    PARTITION_NAME=$i

	    echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Creating "$PARTITION", "$i" script..."

	    BASE_OUTPUT_DIR=$PARTITION_OUTPUT_DIR
		mkdir -p $BASE_OUTPUT_DIR

		SCRIPT_NAME=$BASE_OUTPUT_DIR'/partition_'$PARTITION"_"$i'_bowtie2.sh'

		#	Remove old copy
		rm -f $SCRIPT_NAME

		NEW_SCRIPT=`sed s/PARTITION_MAJOR_NUMBER_PLACEHOLDER/$PARTITION/g $TEMPLATE_SCRIPT_PATH | \
					sed s/PARTITION_MINOR_NUMBER_PLACEHOLDER/$PARTITION_NAME/g`

		echo "$NEW_SCRIPT" >> $SCRIPT_NAME

		chmod ug+rwx $SCRIPT_NAME
    done

    echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
}



echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Finish."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo ""
