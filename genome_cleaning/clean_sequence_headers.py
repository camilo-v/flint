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
#	    The purpose of this program is to clean the sequence headers of the FASTA files in the Ensembl Genome Index.
#       The sequence headers in their initial format are useless for mapping purposes as they do not have a format
#       that can be easily parsed by the Reducer steps in the main Flint MapReduce pipeline.
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

sys.stdout.flush()
print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Python Starting" + "" )


# --------------------------------------------------------- Main ----------------------------------------------------------------
#
# 	Pick up the command line arguments
#

parser = argparse.ArgumentParser()
parser.add_argument( "-i", "--input_file", required=True, type=str, help="Input File with sequence headers." )
parser.add_argument( "-o", "--output_dir", required=False, type=str, help="Output Directory" )
args = parser.parse_args()

filePathForInputFile    = args.input_file
output_directory        = args.output_dir if args.output_dir is not None else "./out"

filePathForInputFile.strip()

# ----------------------------------------------- Output Files & Directories ----------------------------------------------------
#
# We'll check if the output directory exists — either the default (current) or the requested one
#
if not os.path.exists( output_directory ):
    print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Output directory does not exist.  Creating..." + "" )
    os.makedirs( output_directory )

outputFile = output_directory + "/" + "clean_sequence_headers.txt"


# ------------------------------------------------------ File Loading -----------------------------------------------------------

print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Loading Files..." + "" )

numberOfLinesInFile = 0

with open( filePathForInputFile,'r' ) as INFILE:

    reader = csv.reader(INFILE, delimiter=' ')
    writer = csv.writer(open(outputFile, "wb"), delimiter='\t', lineterminator="\n")

    try:
        for row_line in reader:

            newRowArray = []

            for (index, valueOfCell) in enumerate(row_line):

                if index == 2:

                    sequenceAttributes = valueOfCell.split(":")

                    assemblyID      = sequenceAttributes[1]
                    sequenceID      = sequenceAttributes[2]
                    sequenceStart   = sequenceAttributes[3]
                    sequenceEnd     = sequenceAttributes[4]

                    newFastaHeaderString = ">" + assemblyID + "_" + sequenceID + "_" + sequenceStart + "_" + sequenceEnd
                    newRowArray.append(newFastaHeaderString)

            writer.writerow(newRowArray)

            numberOfLinesInFile += 1

    except csv.Error as e:
        sys.exit("File %s, line %d: %s" % (filePathForInputFile, reader.line_num, e))

print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" +
      " Number of Lines in file: " + '{:0,.0f}'.format(numberOfLinesInFile) + "")

# ------------------------------------------------------ End of Line ------------------------------------------------------------

print( "[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Done." + "\n" )

sys.exit()
