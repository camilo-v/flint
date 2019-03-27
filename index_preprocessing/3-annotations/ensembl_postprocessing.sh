#!/bin/bash
# ---------------------------------------------------------------------------------------------------------------------
#
#                                   	Bioinformatics Research Group
#										   http://biorg.cis.fiu.edu/
#                             		  Florida International University
#
#   OBJECTIVE:
#       Post process the lineage file by:
#       1) Adding the correct strain name column
#			and remove the first word from the species name (redundant information - represent genus))
#		2) Calculate the length of each genome and add corresponding column to annotation file
#
#	AUTHOR:	Vitalii Stebliankin (vsteb002@fiu.edu)
#			Bioinformatics Research Group,
#			School of Computing and Information Sciences,
#			Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting postprocessing..." >> running_log.txt
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."

# ----------------------------------- Import variables ----------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $BASE_DIR, $ENSEMBL_VERSION
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi
# ---------------------------------------------------------------------------------------------------------------------
# Directories and constants:
# ---------------------------------------------------------------------------------------------------------------------

# Directory where all the annotations stored:
ANNOTATION_DIR=$BASE_DIR"/annotations"

# Metadata in json format
SPECIES_METADATA=$ANNOTATION_DIR"/assembly_summary.txt"

#the main lineage file that we want to change
ANNOTATION_FILE=$ANNOTATION_DIR"/ensembl_v"$ENSEMBL_VERSION"_lineage.tsv"

# Temporary annotation file to store the changes:
TMP_ANNOTATION=$ANNOTATION_DIR"/tmp_annotation.tsv"

# FASTA file
FASTA=$BASE_DIR"/indices/ensembl_bacterial_v"$ENSEMBL_VERSION"-clean.fasta"
# File with all FASTA headers
SEQ_HEADERS=$BASE_DIR"/annotations/ensembl-seq_headers.txt"

# ---------------------------------------------------------------------------------------------------------------------
# Step 1 - Adding the correct strain name column
# ---------------------------------------------------------------------------------------------------------------------
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Step 1 - Adding the correct strain name column..."
# a) Download assembly_summary.txt if file is not already exist
# assembly_summary.txt is a metadata from genbank that is publically availiable at
# ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/assembly_summary.txt

if [ ! -f $SPECIES_METADATA ]; then
	wget -P $ANNOTATION_DIR ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/assembly_summary.txt
fi

# b) Run python script to do the parsing of metadata
python ensembl_postprocessing_python/add_strain_column.py --lineage_file $ANNOTATION_FILE \
 							--assembly_summary $SPECIES_METADATA \
 							--output $TMP_ANNOTATION

# c) Remove original lineage file:
rm $ANNOTATION_FILE

# ---------------------------------------------------------------------------------------------------------------------
# Step 2 - Calculate the length of each genome and add corresponding column to annotation file
# ---------------------------------------------------------------------------------------------------------------------
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Step 2 - Calculate the length of each genome and add corresponding column to annotation file..."

# All FASTA headers are in format "GCA number"_"SequenceID"_"start coordinate"_"end coordinate"_"Taxonomy ID".
# Therefore, they are useful for calculating the length of genomes

# a) Create file with all FASTA sequence headers:

if [ ! -f $SEQ_HEADERS ]; then
	for name in $(grep ">" $FASTA); do
	echo "${name//>}" >> $SEQ_HEADERS
done
fi

# b) Run python script to calculate the length

python ensembl_postprocessing_python/add_length_to_annotation.py --lineage_file $TMP_ANNOTATION \
																--sequence_headers $SEQ_HEADERS \
																--output $ANNOTATION_FILE
if [ ! -f $ANNOTATION_FILE ]; then
	rm $TMP_ANNOTATION
fi

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done postprocessing." >> running_log.txt
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
