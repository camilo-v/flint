#!/usr/bin/python
# coding: utf-8
# ---------------------------------------------------------------------------------------------------------------------
#
#                                       Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this file is to merge pairs of paired-end reads stored in two (2) files into one (1) single
#   file in "tab5" format. The tab5 format is:
#       [name]\t[seq1]\t[qual1]\t[seq2]\t[qual2]\n
#   The reason for this conversion is so that we can easily use pair-end reads in Spark.
#
#   NOTES:
#   The tab5 format does away with the "@" prefix for the read name, as well as the "/1" or "/2" suffixes.
#
#   DEPENDENCIES:
#
#       • Python & the modules listed below
#
#   You can check the python modules currently installed in your system by running: python -c "help('modules')"
#
#   USAGE:
#       Run the program with the "--help" flag to see usage instructions.
#
#	AUTHOR:
#           Camilo Valdes (camilo@castflyer.com)
#			Florida International University (FIU)
#
#
# ---------------------------------------------------------------------------------------------------------------------

# 	Python Modules
import os, sys
import time
import argparse
import csv
from Bio.SeqIO.QualityIO import FastqGeneralIterator

# -------------------------------------------------------- Main -------------------------------------------------------
#
#
def main(args):

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Starting FASTQ to TAB5 Conversion...")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")

    parser = argparse.ArgumentParser()
    parser.add_argument("--mate_1", required=True, type=str, help="Mate 1 of pair, in FASTQ format.")
    parser.add_argument("--mate_2", required=True, type=str, help="Mate 2 of pair, in FASTQ format.")
    parser.add_argument("--affix", required=False, type=str, help="Affix for Output files.")
    parser.add_argument("--out", required=False, type=str, help="Output Directory.")

    args = parser.parse_args(args)

    filePathForMate1 = args.mate_1
    filePathForMate2 = args.mate_2
    filePathForMate1.strip()
    filePathForMate2.strip()

    affixForOutputFile = args.affix if args.affix is not None else "merged-mates"
    output_directory = args.out if args.out is not None else "./out"

    # ------------------------------------------ Output Files & Directories -------------------------------------------
    #
    #	We'll check if the output directory exists — either the default (current) or the requested one.
    #
    if not os.path.exists(output_directory):
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] " +
              "Requested output directory does not exist.  Creating..." + "")
        os.makedirs(output_directory)

    outputFile = output_directory + "/" + affixForOutputFile + "-tab5.txt"

    # ----------------------------------------------- TAB5 Conversion -------------------------------------------------
    #
    #   TAB5 format: [name]\t[seq1]\t[qual1]\t[seq2]\t[qual2]\n
    #
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Reading Mate 1...")

    dictionaryWithMate1 = {}

    with open(filePathForMate1) as mate_1_in_handle:
        for title, seq, qual in FastqGeneralIterator(mate_1_in_handle):
            clean_title = title.replace('/1', '')
            clean_read = [clean_title, seq, qual]
            dictionaryWithMate1[clean_title] = clean_read

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Reading Mate 2...")

    writer = csv.writer(open(outputFile, "wb"), delimiter='\t', quoting=csv.QUOTE_NONE, lineterminator="\n")

    with open(filePathForMate2) as mate_2_in_handle:
        for title, seq, qual in FastqGeneralIterator(mate_2_in_handle):
            clean_title = title.replace('/2', '')
            mate_1 = dictionaryWithMate1[clean_title]
            writer.writerow([mate_1[0], mate_1[1], mate_1[2], seq, qual])

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Done.")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")


# ----------------------------------------------------------- Init ----------------------------------------------------
#
#   App Initializer.
#
if __name__ == "__main__":
    main(sys.argv[1:])
