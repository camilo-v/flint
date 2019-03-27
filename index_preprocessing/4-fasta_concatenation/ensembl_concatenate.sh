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
#	The purpose of this script is to concatenate a collection of genome files downloaded from the Ensembl Bacteria
#	project website ("http://bacteria.ensembl.org"). The files to concatenate are FASTA files for each genome, and
#	the result will be one large FASTA file with all the sequences in them.
#	During concatenation the script applies several actions:
# 	1)	Clean the sequence headers of the FASTA files in the Ensembl Genome Index.
# 	    The sequence headers in their initial format are useless for mapping purposes as they do not have a format
# 	    that can be easily parsed by the Reducer steps in the main Flint MapReduce pipeline.
# 	2)	Create annotation file with all assemblies.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#		
#		* concatenate.py
#
#
#	AUTHORS:	Camilo Valdes (cvalde03@fiu.edu), Vitalii Stebliankin (vsteb002@fiu.edu)
#				Bioinformatics Research Group,
#				School of Computing and Information Sciences,
#				Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting concatenating..." >> time_log.txt


# ----------------------------------- Import variables ---------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $ENSEMBL_VERSION, $BASE_DIR
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi
# ---------------------------------------------------------------------------------------------------------------------

#	Directory to look for DNA sequences
WORKING_DIR=$BASE_DIR"/fasta/dna"

OUTPUT_DIR=$BASE_DIR"/indices"
mkdir -p $OUTPUT_DIR

#	Directory to look for python script
APP_DIR=$SCRIPTS_DIR'/4-fasta_concatenation'

#	Annotation file from http://bacteria.ensembl.org/info/website/ftp/index.html
ANNOTATIONS_DIR=$BASE_DIR"/annotations" 




python $APP_DIR"/concatenate.py" --input_dir $WORKING_DIR \
								--db_version $ENSEMBL_VERSION \
								--output_dir $OUTPUT_DIR \
								--annotations_dir $ANNOTATIONS_DIR 

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done." >> time_log.txt
