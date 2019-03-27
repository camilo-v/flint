#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                   	Bioinformatics Research Group
#										   http://biorg.cis.fiu.edu/
#                             		  Florida International University
#
#   OBJECTIVE:
#   The purpose of the script is to show statistics
#   (how many unique elements of each phylogenetic tree our database contains)
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#       • get_statistics.py
#
#
#	AUTHOR:	Vitalii Stebliankin (vsteb002@fiu.edu)
#			Bioinformatics Research Group,
#			School of Computing and Information Sciences,
#			Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------


# ----------------------------------- Import variables ---------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $ENSEMBL_VERSION, $BASE_DIR, $SCRIPTS_DIR
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi
# ---------------------------------------------------------------------------------------------------------------------

APP_DIR=$SCRIPTS_DIR"/show_statistics"

ASSEMBLY_FILE=$BASE_DIR"/annotations/assemblies_v"$ENSEMBL_VERSION".txt"

TAX_TREE=$BASE_DIR"/annotations/ensembl_v"$ENSEMBL_VERSION"_lineage.tsv"

python $APP_DIR/"get_statistics.py" --tax_tree $TAX_TREE

assemblies=$(cat $ASSEMBLY_FILE | wc -l)
complete_genomes=$(grep .chromosome. $ASSEMBLY_FILE | wc -l)
echo -e "\t assemblies: "$assemblies
echo -e "\t complete genomes:"$complete_genomes
