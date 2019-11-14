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
#	Download collection of genome FASTA files from the Ensembl Bacteria project website
#	("http://bacteria.ensembl.org").
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#	Change ../config.cfg file to set up your working directory
#
#   DEPENDENCIES:
#
#       • curl
#
#
#	AUTHORS:	Camilo Valdes (cvalde03@fiu.edu), Vitalii Stebliankin (vsteb002@fiu.edu)
#				Bioinformatics Research Group,
#				School of Computing and Information Sciences,
#				Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

# --accept      defines the file types to focus on
# --cut-dirs    defines what directory structure to keep or omit, from host onwards
# -r            Recursively download the directory.
# -nH           Disable generation of host-prefixed directories.
# --no-parent 	Do not ever ascend to the parent directory when retrieving recursively.

# ----------------------------------- Import variables ----------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $ENSEMBL_VERSION, $BASE_DIR
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting Downloads..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

FTP_LOCATION="ftp://ftp.ensemblgenomes.org/pub/release-"${ENSEMBL_VERSION}"/bacteria/fasta/"

OUTPUT_DIR=${BASE_DIR}"/fasta"
mkdir -p ${OUTPUT_DIR}

ACCEPT_PATTERN_DNA='*.dna.toplevel.fa.gz'

echo ${FTP_LOCATION}

for COLLECTION in $(curl -l $FTP_LOCATION)
do
	#   Download the DNA directories
    #
    #   --accept      	Defines the file types to focus on
    #   --cut-dirs    	Defines what directory structure to keep or omit, from host onwards
    #   -r            	Recursively download the directory.
    #   -nH           	Disable generation of host-prefixed directories.
    #   --no-parent   	Do not ever ascend to the parent directory when retrieving recursively.
    #   --mirror      	Among other things, it makes downloads lighting fast!
    #	--no-clobber  	Skip Downloading if files exist.
    #	--timestamping	Don't re-retrieve files unless newer than local.
    #
	wget --accept=${ACCEPT_PATTERN_DNA} -r -nH --cut-dirs=7 --no-parent --quiet -P ${OUTPUT_DIR} ${FTP_LOCATION}${COLLECTION}

done


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Downloads Done."
echo ""
