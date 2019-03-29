# -*- coding: utf-8 -*-
"""
Objective:
    The purpose of this script is to get taxonomic tree:
    1) The taxIDs of input file are compared with taxonomic lineage annotations of all organisms
        (The lineage file can be downloaded through NCBItax2lin tool [1])

    2) The taxIDs that couldn't be found in lineage annotation file are fetched trough the NCBI portal
        (using the script fetch_ncbi.py)


Dependencies:
    * fetch_ncbi.py
    * pandas

Author: Vitalii Stebliankin (vsteb002@fiu.edu)
            Florida International University
            Bioinformatics Research Group

References:
    [1] https://github.com/zyxue/ncbitax2lin (Zhuyi Xue)
"""

import pandas as pd
from fetch_ncbi import get_lineage_file
from fetch_ncbi import ncbi_query
import os
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    help_out = (
    'the output file containing the lineage columns and other custom columns (see parameter --include_columns)')

    parser.add_argument("--input", required=True, type=str, help='a file containing a column with NCBI taxID')
    parser.add_argument("--input_skiprows", required=False, type=int, help='Optional. Number of rows to skip in reading input file. By default set to 0.')
    parser.add_argument("--lineage_file", required=False, type=str,
                        help='a taxonomy annotation file (can be downloaded using https://github.com/zyxue/ncbitax2lin)')
    parser.add_argument("--output", required=True, type=str, help=help_out)
    parser.add_argument("--tax_col", required=True, type=int,
                        help='the position of column with taxID (counting starts from 0)')
    parser.add_argument("--include_columns", required=False, type=str,
                        help="Optional. the column numbers from input file to be included in output file. Column numbers should be comma sepparated in string format. Ex. \"1,4\"   ")
    parser.add_argument("--include_columns_names", required=False, type=str,
                        help="Optional. Rename included columns. By default the program uses column names from the input file")
    parser.add_argument("--ranks", required=False, type=str,
                        help='Optional. Specifies which ranks to use with coma sepparator. By default: --ranks superkindom,phylum,class,order,family,species')
    parser.add_argument("--email", required=False, type=str,
                        help='Optional. Set the Entrez email parameter (default is not set)')
    args = parser.parse_args()

    # -----------------------------------------------------------------------------------------------------------------------
    #   Check Arguments
    # -----------------------------------------------------------------------------------------------------------------------
    if not args.ranks:
        ranks = ['phylum', 'class', 'order', 'family', 'genus', 'species']
    else:
        ranks = args.ranks.split(',')

    if args.include_columns_names:
        # Check if include_columns and include_columns_names has the same length
        if (len(args.include_columns.split(",")) != len(args.include_columns_names.split(","))):
            raise ValueError("Length of include_columns and include_columns_names are not matched")
    tax_col = args.tax_col
    output = args.output

    if args.email:
        email = args.email
    else:
        email = None
    if not args.input_skiprows:
        input_skiprows=0
    else:
        input_skiprows=args.input_skiprows

    # -----------------------------------------------------------------------------------------------------------------------
    #   Read files
    # -----------------------------------------------------------------------------------------------------------------------
    if args.lineage_file:
        # Read lineage file:

        # All columns:
        col_names = ['tax_id', 'superkingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'family1',
                     'forma', 'genus1', 'infraclass', 'infraorder', 'kingdom', 'no rank', 'no rank1', 'no rank10',
                     'no rank11', 'no rank12', 'no rank13', 'no rank14', 'no rank15', 'no rank16', 'no rank17',
                     'no rank18', 'no rank19', 'no rank2', 'no rank20', 'no rank21', 'no rank22', 'no rank3',
                     'no rank4', 'no rank5', 'no rank6', 'no rank7', 'no rank8', 'no rank9', 'parvorder',
                     'species group', 'species subgroup', 'species1', 'subclass', 'subfamily', 'subgenus', 'subkingdom',
                     'suborder', 'subphylum', 'subspecies', 'subtribe', 'superclass', 'superfamily', 'superorder',
                     'superorder1', 'superphylum', 'tribe', 'varietas']

        #   Read lineage file
        all_lineage_df = pd.read_csv(args.lineage_file, dtype=str)
        #
        #   Reducing number of columns:
        lineage_cols = ['tax_id'] + ranks
        all_lineage_df =all_lineage_df[lineage_cols]

        # Read Target File with Tax IDs
        target_df = pd.read_table(args.input, skiprows=input_skiprows, index_col=False)

        # Establish desired columns that was specified in --include_columns:
        target_df_names = target_df.columns.values.tolist()
        desired_columns = [target_df_names[tax_col]]
        if args.include_columns:
            target_include_columns_index = args.include_columns.split(",")
            desired_columns += [target_df_names[int(x)] for x in target_include_columns_index]

        # Remove unwanted columns from target_df:
        target_df = target_df[desired_columns]

        # Rename columns:
        if args.include_columns_names:
            new_names = args.include_columns_names.split(",")

            for i, index in enumerate(target_include_columns_index):
                target_df = target_df.rename(columns={target_df_names[int(index)]: new_names[i]})
        # -----------------------------------------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------------------------------------------------
        #   Merge all_lineage_df and target_df
        # -----------------------------------------------------------------------------------------------------------------------

        # Rename tax_id column:
        target_df = target_df.rename(columns={target_df_names[tax_col]: "tax_id"})

        # Transform tax_id column to the same format
        target_df["tax_id"] = target_df["tax_id"].apply(int)
        all_lineage_df["tax_id"] = all_lineage_df["tax_id"].apply(int)
        # the lineage that not available set to "unclassified":
        all_lineage_df = all_lineage_df.fillna("unclassified")

        # Merge our df with all lineage df
        target_lineage_df = target_df.merge(all_lineage_df, left_on="tax_id", right_on="tax_id", how="left")

        # Data Frame with tax_id that is not found in all_lineage_df:
        to_fetch_df = target_lineage_df[target_lineage_df["species"].isnull()]
        genbank_lineage_df = target_lineage_df.dropna(subset=["species"])

        # -----------------------------------------------------------------------------------------------------------------------
        # Write current version of lineage_df to file
        genbank_lineage_df.to_csv(output, sep="\t", index=False)

        if len(to_fetch_df) > 0:
            # -----------------------------------------------------------------------------------------------------------------------
            # Write to_fetch_df to tmp_fetch.txt
            to_fetch_df.to_csv("tmp_fetch.txt", sep="\t", index=False)

            # -----------------------------------------------------------------------------------------------------------------------
            # Fetch tmp_fetch.txt from NCBI:
            if args.include_columns:
                tmp_names = to_fetch_df.columns.values.tolist()
                include_columns_tmp = ""
                include_cols_names_tmp = ""
                for i in range(1, len(desired_columns)):
                    include_columns_tmp += str(i) + ","
                    include_cols_names_tmp += str(tmp_names[i]) + ","
                include_columns_tmp = include_columns_tmp.strip(",")
                include_cols_names_tmp = include_cols_names_tmp.strip(",")
            else:
                include_columns_tmp = args.include_columns
                include_cols_names_tmp = args.include_columns_names

            get_lineage_file(tax_file="tmp_fetch.txt",
                             ranks=ranks,
                             tax_col=0,
                             include_columns=include_columns_tmp,
                             include_columns_names=include_cols_names_tmp,
                             output=output,
                             email=email,
                             append=True)
            # -----------------------------------------------------------------------------------------------------------------------
            # Remove tmp_fetch.txt
            os.remove("tmp_fetch.txt")
            # -----------------------------------------------------------------------------------------------------------------------
    # If there is no lineage file provided - perform fetching of the whole input through NCBI
    else:
        get_lineage_file(args.input, ranks, args.tax_col, args.include_columns, args.include_columns_names, args.output,
                         args.email)
