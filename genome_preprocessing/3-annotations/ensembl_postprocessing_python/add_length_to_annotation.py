#----------------------------------------------------------------------------------------
# Objective:
#   Part of the postprocess annotations pipeline
#	Add length column to phylogenetic annotation file
#
# Dependencies:
#   * Pandas
#   * Argparse
#
# Author:
#	Vitalii Stebliankin (vsteb002@fiu.edu)
#           Florida International University
#           Bioinformatics Research Group
#----------------------------------------------------------------------------------------

import pandas as pd
import argparse

def calculate_length_assembly(start, end):
    return int(end)-int(start)+1 #+1 because the start coordinate is 1

parser=argparse.ArgumentParser()

parser.add_argument("--lineage_file", required=True, help="Path to annotation lineage file")
parser.add_argument("--sequence_headers", required=True, help="Path to sequence headers" )
parser.add_argument("--output", required=True, help="Path to output file")

args=parser.parse_args()

TAXTREE_PATH=args.lineage_file
SEQUENCE_HEADERS_PATH=args.sequence_headers
output=args.output

# TAXTREE_PATH="data/tmp.tsv"
# SEQUENCE_HEADERS_PATH="data/ensembl-seq_headers.txt"
# output="data/out.tsv"



# Read taxtree

#----------------------------------------------------------------------------------------
# Read data frames
#----------------------------------------------------------------------------------------

# 1) Read sequence headers
all_assemblies_df = pd.read_csv(SEQUENCE_HEADERS_PATH, sep="_", header=None, dtype=str,
                                 names=["assembly_accession", "seq_name", "start", "end", "taxID"])
all_assemblies_df["assembly_accession"] = all_assemblies_df["assembly_accession"].apply(lambda x: x.replace(">", ""))
# 2) Read taxtree
taxtree_df = pd.read_table(TAXTREE_PATH, dtype=str)


#----------------------------------------------------------------------------------------
# Calculate the length of each genome
#----------------------------------------------------------------------------------------

# 1) Calculate length of each assembly:
all_assemblies_df["length"] = all_assemblies_df.apply(lambda row: calculate_length_assembly(row["start"], row["end"]),
                                                      axis=1)

# 2) calculate length of each genome:
all_assemblies_df = all_assemblies_df.pivot_table(index=["assembly_accession", "taxID"], values=["length"], aggfunc=sum)
all_assemblies_df["assembly_accession"] = all_assemblies_df.index.get_level_values("assembly_accession")
all_assemblies_df["taxID"] = all_assemblies_df.index.get_level_values("taxID")
all_assemblies_df = all_assemblies_df.reset_index(drop=True)

#----------------------------------------------------------------------------------------
# Merge length with taxtree file
#----------------------------------------------------------------------------------------

# 1) Add "GCA_" to each of the genome
all_assemblies_df["assembly_accession"] = all_assemblies_df["assembly_accession"].apply(lambda x: "GCA_"+x)

# 2) Merge Assemblies
merged_df = taxtree_df.merge(all_assemblies_df, how="left", on="assembly_accession")
merged_df = merged_df.drop("taxID", axis=1)

# Remove decimals

merged_df["length"] = merged_df["length"].apply(str)
merged_df["length"] = merged_df["length"].apply(lambda x: x.split(".")[0])

merged_df.to_csv(output, sep="\t", index=False)