#!/bin/bash
#----------------------------------------------------------------------------------------
# Objective:
#	Create lineage annotations for GB full database using summary file containing tax_id column
#
# Dependencies:
#	* taxIDtoLineage (https://github.com/stebliankin/taxIDtoLineage)
#	* assembly_summary.txt - downloaded from ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/assembly_summary.txt
#	* lineages_bacteria.csv - downloaded using ncbitax2lin (https://github.com/zyxue/ncbitax2lin)
#
#
# Author:
#	Vitalii Stebliankin (vsteb002@fiu.edu)
#           Florida International University
#           Bioinformatics Research Group
#----------------------------------------------------------------------------------------

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting lineage_full.sh..." >> running_log.txt
python3 taxIDtoLineage/taxIDtoLineage.py --input ../../annotations/assembly_summary.txt \
						--input_skiprows 1 \
                       --lineage_file ../../annotations/lineages_bacteria.csv \
                       --output ../../annotations/full_lineage.tsv \
                       --tax_col 5 \
                       --include_columns "0,4,7,11" \
                       --include_columns_names "assembly_accession,refseq_category,strain,assembly_level" \
                       --email "your@email"

echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done with lineage_full.sh" >> running_log.txt
