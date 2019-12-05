#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Objective:
    The purpose of this script is to get taxonomic tree from given file contaning the NCBI taxID column.


Required arguments:
    --input                 (str)       a file containing a column with NCBI taxID
    --output                (str)       the output file containing the lineage columns
                                        ('superkingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species')
                                        and other custom columns (see parameter --include_columns)

    --tax_col               (int)       the position of column with taxID (counting starts from 0).

Optional arguments:
    --include_columns       (str)       the columns from input file to be included to output file
                                        sequence of int with comma delimiter
                                        "1,2,4" - include columns 1, 2, and 4 from input file to the result file.

    --include_columns_names (str)       Column names of included columns.
                                        By default the program uses column names from the input file.
                                        Names should be comma separated.
                                        For example, if include_columns="1,4"
                                        --include columns_names "assemblyID,strain" will rename
                                        the default column names from the input.

    --email                 (str)       Set the Entrez email parameter (default is not set)

    --ranks                 (str)       Optional. Specifies which ranks to use with coma sepparator.
                                        By default: --ranks superkindom, phylum, class, order, family, species


DEPENDENCIES:
    Biopython

Author: Vitalii Stebliankin (vsteb002@fiu.edu)
            Florida International University
            Bioinformatics Research Group
"""

import csv
from Bio import Entrez
import os
import os.path
import time
import argparse
import urllib


#----------------------------------------------------------------------------
# Do a single query to NCBI that returns list of taxonomic tree from given taxID
#----------------------------------------------------------------------------
def ncbi_query(taxID, ranks):
    handle = Entrez.efetch(db='Taxonomy', id=taxID, rettype='gb', retmode='xml')
    record = Entrez.read(handle)
    lineageEx = record[0]['LineageEx']
    rank_record = {'superkingdom': '', 'phylum': '', 'class': '',
                   'order': '', 'family': '', 'genus': '', 'species': ''}
    for element in lineageEx:
        if element['Rank'] in ranks:
            rank_record[element['Rank']] = element['ScientificName']
    if rank_record['species'] == '':
        rank_record['species'] = record[0]['ScientificName']
    list_rank = []
    for key in ranks:
        if rank_record[key] != '':
            list_rank.append(rank_record[key])
        else:
            list_rank.append('unclassified')
    return list_rank


#----------------------------------------------------------------------------
# The main function for getting lineage.
#----------------------------------------------------------------------------
def get_lineage_file(tax_file, ranks, tax_col, include_columns, include_columns_names, output, email, append=False):

    # set up the file to write HTTP request failures:
    HTTP_FAILURES=os.path.join(os.path.dirname(output), "HTTP_request_failures.txt")
    start_time = time.time()
    if not append:
        if os.path.isfile(output):
            os.remove(output)
        if os.path.isfile(HTTP_FAILURES):
            os.remove(HTTP_FAILURES)
    if email:
        Entrez.email = email
    if include_columns:
        col_list = include_columns.split(',')
        col_list = [int(x) for x in col_list]
    else:
        col_list=[]
    with open(tax_file) as tsv:
        reader = csv.reader(tsv, delimiter='\t')
        taxID=0 #assign to make first comparasig
        for i, row in enumerate(reader):
            if i==0:
                if not append:
                    if not include_columns_names:
                        include_columns_names_list = [row[x] for x in col_list]
                    else:
                        include_columns_names_list = include_columns_names.split(",")
                    names = include_columns_names_list + [row[tax_col]] + ranks
                    write_to_file(output, names)
                else:
                    pass
            else:
                if (i % 1000 == 0) and (append==False):
                    print("{} rows computed; computational time: {:.2f} min".format(str(i), (time.time() - start_time)/60, "min"))
                try:
                    if taxID != row[tax_col]:#avoid doing query of repetitive ID
                        #assign taxID
                        taxID = row[tax_col]
                        #assign taxonomic tree
                        list_rank = ncbi_query(taxID, ranks)
                    list_to_write = [row[tax_col]] + [row[x].strip().strip('-') for x in col_list] + list_rank
                    write_to_file(output, list_to_write)
                except urllib.error.HTTPError:
                    print()
                    print("HTTP request error while parsing the following row:")
                    print(row)
                    print("Writing row to HTTP_request_failures.txt for future repeat of parsing")
                    print()
                    write_to_file(HTTP_FAILURES, row)
    if os.path.isfile(HTTP_FAILURES):
        print("Parsing HTTP_request_failures.txt ...")
        for i in range(5):
            if os.path.isfile(HTTP_FAILURES):
                parse_error(output, ranks, tax_col, include_columns, include_columns_names, email)
        if os.path.isfile(HTTP_FAILURES):
            raise ConnectionError("Can't parse some of the rows. Check HTTP_request_failures.txt for details.")
    end_time = time.time()
    if append == False:
        print('Done. Running time - ', "{:.2f}".format((end_time - start_time)/3600), ' hours.')
    return


#----------------------------------------------------------------------------
# Append row to the file from a given list
#----------------------------------------------------------------------------
def write_to_file(file, list, delimiter='\t'):
    #create row to write:
    toWrite = ''
    for i, element in enumerate(list):
        if i != len(list) - 1:
            toWrite += str(element) + delimiter
        else:
            toWrite += str(element) + '\n'
    #append row to the file
    with open(file, 'a', newline='') as f:
        f.write(toWrite)
    return


def parse_error(filename, ranks, tax_col, include_columns, include_columns_names, email):
    # set up the file to write HTTP request failures:
    HTTP_FAILURES = os.path.join(os.path.dirname(filename), "HTTP_request_failures.txt")
    TMP_FILE = os.path.join(os.path.dirname(filename), "tmp_parsing.txt")
    if os.path.isfile(filename):
        if os.path.isfile(HTTP_FAILURES):
            os.rename(HTTP_FAILURES, TMP_FILE)
            get_lineage_file(TMP_FILE, ranks, tax_col, include_columns, include_columns_names, filename, email, append=True)
            if os.path.exists(TMP_FILE):
                os.remove(TMP_FILE)
        else:
            print('HTTP_request_failures.txt is not found')
            exit()
    else:
        print('Error', filename, 'is not found')
        exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    #message for output help:
    help_out =  ('the output file containing the lineage columns and other custom columns (see parameter --include_columns)')

    # parse arguments
    parser.add_argument("--input", required=True, type=str, help='a file containing a column with NCBI taxID')
    parser.add_argument("--output", required=True, type=str, help=help_out)
    parser.add_argument("--tax_col", required=True, type=int, help='the position of column with taxID (counting starts from 0)')
    parser.add_argument("--include_columns", required=False, type=str, help="Optional. the column numbers from input file to be included in output file. Column numbers should be comma sepparated in string format. Ex. \"1,4\"   ")
    parser.add_argument("--include_columns_names", required=False, type=str, help="Optional. Column names of included columns. By default the program uses column names from the input file")
    parser.add_argument("--ranks", required=False, type=str, help='Optional. Specifies which ranks to use with coma sepparator. By default: --ranks superkindom,phylum,class,order,family,species')
    parser.add_argument("--email", required=False, type=str, help='Optional. Set the Entrez email parameter (default is not set)')
    args = parser.parse_args()

    if not args.ranks:
        ranks = ['phylum', 'class', 'order', 'family', 'genus', 'species']
    else:
        ranks = args.ranks.split(',')
    if args.include_columns_names:
        # Check if include_columns and include_columns_names has the same length
        if (len(args.include_columns.split(",")) != len(args.include_columns_names.split(","))):
            raise ValueError("Length of include_columns and include_columns_names are not matched")

    #do query to NCBI and write taxonomic tree to OUTPUT file
    get_lineage_file(args.input, ranks, args.tax_col, args.include_columns, args.include_columns_names, args.output, args.email)
