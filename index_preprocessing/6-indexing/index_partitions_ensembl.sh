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
#	The purpose of this script is to index a collection of bacterial genomes stored in a fasta file with the
#	bowtie2-build index builder program.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#       • Bowtie2
#
#
#	AUTHORS:	Camilo Valdes (cvalde03@fiu.edu), Vitalii Stebliankin (vsteb002@fiu.edu)
#				Bioinformatics Research Group,
#				School of Computing and Information Sciences,
#				Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting of full_index_partitions_bwt2.sh..." >> time_log.txt

# ----------------------------------- Import variables ---------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $ENSEMBL_VERSION, $BASE_DIR, $SCRIPTS_DIR
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi
# ---------------------------------------------------------------------------------------------------------------------


NUMBER_OF_PARTITIONS=64
NUMBER_OF_THREADS=32

BASE_DIR=$BASE_DIR"/indices"

INDEX_NAME="ensembl_v"$ENSEMBL_VERSION


PARTITIONS_ARRAY=( $(seq 1 $NUMBER_OF_PARTITIONS) )

for PARTITION in ${PARTITIONS_ARRAY[@]}; do
	echo "	[" `date '+%m/%d/%y %H:%M:%S'` "] indexing partition $PARTITION"
	echo "	[" `date '+%m/%d/%y %H:%M:%S'` "] indexing partition $PARTITION" >> time_log.txt
	OUTPUT_DIR=$BASE_DIR"/partitions_"$NUMBER_OF_PARTITIONS"/$PARTITION"
	FASTA_FILE=$OUTPUT_DIR"/"$INDEX_NAME".fasta"
	bowtie2-build -f --threads $NUMBER_OF_THREADS $FASTA_FILE $OUTPUT_DIR"/"$INDEX_NAME
done
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done." >> time_log.txt

