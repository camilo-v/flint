# ---------------------------------------------------------------------------------------------------------------------
#
#                                   	Bioinformatics Research Group
#										   http://biorg.cis.fiu.edu/
#                             		  Florida International University
#
#   OBJECTIVE:
#       1) Add strain column to Ensembl lineage annotation file by merging lineage file with metadata from GenBank
#       2) Remove first word from the species name (redundant information - represent genus)
#
#   DEPENDENCIES:
#       * Pandas
#
#	AUTHOR:	Vitalii Stebliankin (vsteb002@fiu.edu)
#			Bioinformatics Research Group,
#			School of Computing and Information Sciences,
#			Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

import pandas as pd
import argparse

def remove_word_strain(strain):
    if not pd.isnull(strain):
        strain=strain.split("=")[1]
    return strain

def remove_word_species(word):
    if word!="unclassified":
        alist=word.split(" ")[1:]
        new_word=""
        for item in alist:
            new_word+=item+" "
        word=new_word.strip(" ")
    return word

def append_to_genus(species):
    genus=species.split(" ")[0].strip("[").strip("]")
    return genus

# ---------------------------------------------------------------------------------------------------------------------
# Picking up arguments
# ---------------------------------------------------------------------------------------------------------------------
#
parser=argparse.ArgumentParser()

parser.add_argument("--lineage_file", required=True, help="Path to annotation lineage file")
parser.add_argument("--assembly_summary", required=True, help="Path to assembly_summary.txt")
parser.add_argument("--output", required=True, help="Path to the file to write output")

args = parser.parse_args()

assembly_summary=args.assembly_summary
lineage_file=args.lineage_file
output=args.output


# ---------------------------------------------------------------------------------------------------------------------
# Read input files
# ---------------------------------------------------------------------------------------------------------------------

lineage_df = pd.read_table(lineage_file, sep="\t", dtype=str)
lineage_df = lineage_df[['tax_id', 'strain', 'assembly_accession', 'phylum', 'class', 'order', 'family',
 'genus', 'species']]
lineage_df = lineage_df.rename(index=str, columns={"strain":"scientific_name"})
assembly_summary_df=pd.read_table(assembly_summary, sep="\t", dtype=str, skiprows=1)


# ---------------------------------------------------------------------------------------------------------------------
# Getting strain column by merging lineage file with assembly_summary.txt
# ---------------------------------------------------------------------------------------------------------------------

merged_df = lineage_df.merge(assembly_summary_df[["# assembly_accession", "infraspecific_name"]],
                             how="left", left_on="assembly_accession", right_on="# assembly_accession")
merged_df = merged_df.drop("# assembly_accession", axis=1)
merged_df=merged_df.rename(index=str, columns={"infraspecific_name":"strain"})
merged_df["strain"] = merged_df["strain"].apply(remove_word_strain)
merged_df = merged_df.fillna("unclassified")


# ---------------------------------------------------------------------------------------------------------------------
# Remove first word from the species name (redundant information - represent genus)
# ---------------------------------------------------------------------------------------------------------------------

# Add genus from strain if it's unclassified
merged_df["genus"] = merged_df.apply(lambda row: append_to_genus(row["species"]) if row["genus"]=="unclassified" else row["genus"], axis=1 )

# Remove first word of the species:
merged_df["species"] = merged_df.apply(lambda row: remove_word_species(row["species"]), axis=1 )

# ---------------------------------------------------------------------------------------------------------------------
# Write the result in file
# ---------------------------------------------------------------------------------------------------------------------
merged_df.to_csv(output, index=False, sep="\t")
