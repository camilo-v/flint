#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                                   Bioinformatics Research Group
#                                                     http://biorg.cis.fiu.edu/
#                                                  Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	    The purpose of this script is to act as a basic test for the 'split_fasta_file.py' utility.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#		• BioPython
#
#	AUTHORS:
#           Camilo Valdes
#           cvalde03@fiu.edu
#           https://github.com/camilo-v
#
#			Vitalii Stebliankin
#			vsteb002@fiu.edu
#			https://github.com/stebliankin
# ---------------------------------------------------------------------------------------------------------------------

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting test..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting test..." >> time_log.txt
# ----------------------------------- Import variables ---------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $ENSEMBL_VERSION, $BASE_DIR, $SCRIPTS_DIR
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi
# ---------------------------------------------------------------------------------------------------------------------

#   Base location of the main files and the partition output directory
BASE_DIR=$BASE_DIR'/indices'

OUTPUT_DIR=$BASE_DIR'/partitions'

OUTPUT_AFFIX='ensembl'

FASTA_FILE=$BASE_DIR'/ensembl_bacterial_v'$ENSEMBL_VERSION'-clean.fasta'

#   Directory path to the script 'split_fasta_file.py'.
APP_PATH=$SCRIPTS_DIR'/5-partitioning'


#
#   We need multiple partitions to test the cluster
#
ARRAY_OF_PARTITIONS=( '64' )

for PARTITION in "${ARRAY_OF_PARTITIONS[@]}"
{
    echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
    echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Splitting into "$PARTITION

    #   Needs to be a multiple of 2.
    NUMBER_OF_PARTITIONS=$PARTITION

    #
    #	Run the Script
    #
    $APP_PATH/split_fasta_file.py   --file $FASTA_FILE \
                                    --partitions $NUMBER_OF_PARTITIONS \
                                    --affix $OUTPUT_AFFIX \
                                    --version $ENSEMBL_VERSION \
                                    --out $OUTPUT_DIR

}


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Test ended."
echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Test ended." >> time_log.txt
