"""

Objective:
    The purpose of the script is to show statistics
    (how many unique elements of each phylogenetic tree our database contains)

Dependencies:
    * Pandas

Author: Vitalii Stebliankin (vsteb002@fiu.edu)
            Florida International University
            Bioinformatics Research Group
"""
import argparse
import pandas as pd

parser = argparse.ArgumentParser()

parser.add_argument("--tax_tree", required=True, type=str, help='Path to annotation file with taxonomic tree.')
args = parser.parse_args()

annotation_df = pd.read_table(args.tax_tree, dtype=str)

ranks = ['phylum', 'class', 'order', 'family', 'genus', 'species', 'strain']

# Print Statistics
print("Our database contains:")
for key in ranks:
    annotation_df = annotation_df[annotation_df[ranks]!='unclassified']
    counts = annotation_df[key].nunique()

    print("\t {}: {}".format(key, counts))