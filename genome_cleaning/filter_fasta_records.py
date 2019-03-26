#!/usr/bin/python
# coding: utf-8

# -------------------------------------------------------------------------------------------------------------------------------
#
#                                       Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	    The purpose of this program is to filter the Ensembl Bacterial Genome database for "complete" genome records.  The
#       majority of the genomes in Ensembl Bacteria are "SuperContig" chromosomes, or "draft" genomes that have not been
#       completed and contain gaps.  This filter removes those genomes and retains only "finished" genomes, or those genomes
#       that for all intents and purposes are considered completed.  Specifically, genomes that have a "dna:chromosome"
#       attribute are kept, and others (dna:plasmid, and dna:supercontig) are discarded.  Note that the purpose of these
#       finished genomes in our context is to only use them as rough benchmark, and not to use them in a production
#       environment.
#
#
#   NOTES:
#   Please see the dependencies section below for the required libraries (if any).
#
#   DEPENDENCIES:
#       • BioPython
#
#   You can check the python modules currently installed in your system by running: python -c "help('modules')"
#
#   USAGE:
#       Run the program with the "--help" flag to see usage instructions.
#
#	AUTHOR:
#           Camilo Valdes
#           cvalde03@fiu.edu
#           https://github.com/camilo-v
#			Florida International University, FIU
#           School of Computing and Information Sciences
#           Bioinformatics Research Group, BioRG
#           http://biorg.cs.fiu.edu/
#
#
# -------------------------------------------------------------------------------------------------------------------------------

# 	Python Modules (standard)
import os, sys
import argparse
import time
import csv
from Bio import SeqIO
from Bio.SeqRecord import SeqRecord

sys.stdout.flush()
print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Python Starting" + "" )


# --------------------------------------------------------- Main ----------------------------------------------------------------
#
# 	Pick up the command line arguments
#

parser = argparse.ArgumentParser()
parser.add_argument( "--input_file", required=True, type=str, help="Input FASTA file." )
parser.add_argument( "--filter_file", required=True, type=str, help="Filter file with Ensembl Assembly IDs.")
parser.add_argument( "--db_version", required=True, type=str, help="Version of Ensembl Database." )
parser.add_argument( "--output_dir", required=False, type=str, help="Output Directory" )
args = parser.parse_args()

filePathForInputFile    = args.input_file
filePathForFilterFile   = args.filter_file
output_directory        = args.output_dir if args.output_dir is not None else "./out"
ensemblDatabaseVersion  = args.db_version

filePathForInputFile.strip()
filePathForFilterFile.strip()


# ----------------------------------------------- Output Files & Directories ----------------------------------------------------
#
# We'll check if the output directory exists — either the default (current) or the requested one
#
if not os.path.exists( output_directory ):
    print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Output directory does not exist.  Creating..." + "" )
    os.makedirs( output_directory )

outputFASTAFile = output_directory + "/" + "ensembl_bacterial_v" + ensemblDatabaseVersion + "-chromosomes.fasta"


# ----------------------------------------------------- Filter --------------------------------------------------------
#
#   The filter file contains the records that we are interested in. Namely, those of the complete ('finished')
#   genomes (those with no gaps in their sequence).
#

dictionaryWithAssemblyFilters = {}

print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Loading Assembly IDs..." + "" )

numberOfLinesInFile = 0

with open(filePathForFilterFile, 'r') as INFILE:
    reader = csv.reader(INFILE, delimiter='\t')

    try:
        for row_line in reader:
            filterAssemblyID = row_line[0]
            #   Store the Assembly ID so we can look it up later on
            dictionaryWithAssemblyFilters[ filterAssemblyID ] = 1
            numberOfLinesInFile += 1

    except csv.Error as e:
        sys.exit("File %s, line %d: %s" % (filePathForInputFile, reader.line_num, e))

print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" + " Number of IDs: " + '{:0,.0f}'.format(
    numberOfLinesInFile) + "")


# ------------------------------------------------------ File Loading -----------------------------------------------------------

print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Loading Files..." + "" )

for record in SeqIO.parse(filePathForInputFile, "fasta"):
    #   FASTA Record properties contains all the information avalilable for 'this' sequence
    recordProperties        = record.description.split(" ")

    #   DNA Attributes tells us what type of DNA sequence we are dealing with, e.g., Chromosome, Plasmid, Supercontig, etc.
    dnaAttributes           = recordProperties[1].split(":")
    dnaType                 = dnaAttributes[1]

    if assemblyID in dictionaryWithAssemblyFilters and dnaType == 'chromosome':
        with open(outputFASTAFile, "a") as outputHandleForCleanFastaFile:
            SeqIO.write(record, outputHandleForCleanFastaFile, "fasta")


# ------------------------------------------------------ End of Line ------------------------------------------------------------

print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Done." + "\n" )

sys.exit()
