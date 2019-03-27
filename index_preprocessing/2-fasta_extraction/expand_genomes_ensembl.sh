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
#	The purpose of this script is to expand a collection of genome files downloaded from the Ensembl Bacteria
#	project website ("http://bacteria.ensembl.org"). The compressed files are in GZip format (.gz) and the
#	gunzip utility is used to expand these.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#       • gunzip
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
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting expanding..." >> time_log.txt


# ----------------------------------- Import variables ---------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $ENSEMBL_VERSION, $BASE_DIR
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi
# ---------------------------------------------------------------------------------------------------------------------

WORKING_DIR=$BASE_DIR"/fasta"

for aFile in `find $WORKING_DIR -wholename */dna/*.fa.gz`
{
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] File: "$aFile

	gunzip $aFile
}


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done." >> time_log.txt
