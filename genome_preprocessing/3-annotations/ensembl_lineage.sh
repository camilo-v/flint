#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                   	Bioinformatics Research Group
#										   http://biorg.cis.fiu.edu/
#                             		  Florida International University
#
#   OBJECTIVE:
#	Create annotation files for Ensemble Bacteria genome collection
#	1) 	Download metadata on the genomes provided by Ensembl Genomes.
#		Metadate could be found on http://bacteria.ensembl.org/info/website/ftp/index.html
#	3)	Create phylogenetic tree for all genomes in Ensembl database
#
#   NOTES:
#   	Please, configure parameters such as path and email in ../config.cfg
#
#
#	AUTHOR:	Vitalii Stebliankin (vsteb002@fiu.edu)
#			Bioinformatics Research Group,
#			School of Computing and Information Sciences,
#			Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

# ----------------------------------- Import variables ---------------------------------------------------------------
# The script imports config file only if the file is a source of execution.
# Otherwise, import config in the main script
# Variables to import: $ENSEMBL_VERSION, $BASE_DIR, $SCRIPTS_DIR, $taxIDtoLineage_path
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	source ../config.cfg
fi
# ---------------------------------------------------------------------------------------------------------------------

OUTPUT_DIR=$BASE_DIR"/annotations"
mkdir -p $OUTPUT_DIR

APP_DIR=$taxIDtoLineage_path
OUTPUT_TAXONOMY=$OUTPUT_DIR"/ensembl_v"$ENSEMBL_VERSION"_lineage.tsv"

# ---------------------------------------------------------------------------------------------------------------------
#
# Step 1 - Download annotation file species_EnsemblBacteria.txt
#
# ---------------------------------------------------------------------------------------------------------------------
if [ ! -f $OUTPUT_DIR"/species_EnsemblBacteria.txt" ]; then

	echo ""
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]  Downloading annotation file species_EnsemblBacteria.txt..."
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

	FTP_LINK="ftp://ftp.ensemblgenomes.org/pub/release-"$ENSEMBL_VERSION"/bacteria/species_EnsemblBacteria.txt"

	wget -P $OUTPUT_DIR $FTP_LINK
fi
#
# Changing species_EnsemblBacteria.txt
#
# Change unwanted characters on dashes:
#sed 's/_/-/g;s/ /-/g;s/#/-/g;s/\//-/g;s/(/-/g;s/)//g;s/,-/-/g;s/:/-/g;s/\.-/-/g' $OUTPUT_DIR"/species_EnsemblBacteria.txt" > $OUTPUT_DIR"/tmp.txt"
#mv $OUTPUT_DIR"/tmp.txt" $OUTPUT_DIR"/species_EnsemblBacteria.txt"
# Remove spaces:
#sed 's/\./-/g;s/--/-/g' $OUTPUT_DIR"/species_EnsemblBacteria.txt" > $OUTPUT_DIR"/tmp.txt"
#mv $OUTPUT_DIR"/tmp.txt" $OUTPUT_DIR"/species_EnsemblBacteria.txt"
# ---------------------------------------------------------------------------------------------------------------------
#
# Step 2 - Download pregenerated lineage file from https://github.com/zyxue/ncbitax2lin
#
# ---------------------------------------------------------------------------------------------------------------------
if [ ! -f $taxIDtoLineage_path"/all_lineages.csv" ]; then
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Download pregenerated lineage file..."
	echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
	wget -P $taxIDtoLineage_path $LINEAGES_LINK
	gunzip $taxIDtoLineage_path"/"$LINEAGES_VERSION".csv.gz"
	mv $taxIDtoLineage_path"/"$LINEAGES_VERSION".csv" $taxIDtoLineage_path"/all_lineages.csv"
fi
# ---------------------------------------------------------------------------------------------------------------------
#
# Step 3 - # Create taxonomy tree for all bacterias
#
# ---------------------------------------------------------------------------------------------------------------------

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]  Creating taxonomy tree for all bacterias..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"


python3 $taxIDtoLineage_path"/taxIDtoLineage.py" --input $OUTPUT_DIR"/species_EnsemblBacteria.txt" \
						--input_skiprows 0 \
                       --lineage_file $taxIDtoLineage_path"/all_lineages.csv" \
                       --output $OUTPUT_TAXONOMY \
                       --tax_col 3 \
                       --include_columns "0,4,5" \
                       --include_columns_names "strain,assembly,assembly_accession" \
                       --email $EMAIL

echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""
