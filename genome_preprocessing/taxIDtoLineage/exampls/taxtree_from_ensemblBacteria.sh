#!/bin/bash
# --------------------------------------------------------------------------
# Example of usage get_lineage.py
# Script for getting taxonomic tree from EnsemblBacteria metadata file
# Metadata could be downloaded from http://bacteria.ensembl.org/info/website/ftp/index.html
# (wget ftp://ftp.ensemblgenomes.org/pub/release-38/bacteria/species_EnsemblBacteria.txt)
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# initializing constances:
VERSION=38
INPUT='species_EnsemblBacteria.txt'
OUTPUT='tax_tree_'$VERSION'.txt'
TAX_COL=3
INCLUDE_COLUMNS='4,1'
EMAIL='your@email'
# --------------------------------------------------------------------------
python taxIDtoLineage.py --input $INPUT --output $OUTPUT --tax_col $TAX_COL --include_columns $INCLUDE_COLUMNS --email $EMAIL
