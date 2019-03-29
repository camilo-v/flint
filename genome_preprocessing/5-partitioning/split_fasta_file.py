#!/usr/bin/python3

# -------------------------------------------------------------------------------------------------------------------------------
#
#                                                   Bioinformatics Research Group
#                                                     http://biorg.cis.fiu.edu/
#                                                  Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	    The purpose of this program is to split a large fasta file into a user-defined number of partitions so that
#       the resulting smaller files can be indexed by a program such as Bowtie2, or kallisto.  Note that the number of
#       partitions must be a multiple of 2, since the program uses a binary-partitioning strategy.
#
#
#   NOTES:
#   Please see the dependencies section below for the required libraries (if any).  This script uses code, and the general
#   algorithm for splitting large files from http://biopython.org/wiki/Split_large_file.
#
#   DEPENDENCIES:
#
#       Biopython
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
#
# -------------------------------------------------------------------------------------------------------------------------------

# 	Python Modules
import os, sys
import argparse
import time
import math
from Bio import SeqIO

# ------------------------------------------------------------- Functions -------------------------------------------------------
#
#

def batch_iterator(iterator, batch_size):
    """
    Returns lists of length batch_size. This is a generator function, and it returns lists of the entries from the supplied
    iterator.  Each list will have batch_size entries, although the final list may be shorter.This can be used on any
    iterator, for example to batch up SeqRecord objects from Bio.SeqIO.parse(...), or to batch. Alignment objects from
    Bio.AlignIO.parse(...), or simply lines from a file handle.

    Args:
        iterator:   Iterator object to use in the main loop.
        batch_size: The batch we wish to add to the partition file.

    Returns:

    """

    entry = True

    while entry:
        batch = []
        while len( batch ) < batch_size:
            try:
                entry = next(iterator)
            except StopIteration:
                entry = None
            if entry is None:
                # End of file
                break
            batch.append( entry )
        if batch:
            yield batch

# --------------------------------------------------------------- Init ----------------------------------------------------------
#
#   Start
#
if __name__ == "__main__":

    sys.stdout.flush()
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ----------------------------------------------")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Fasta File Splitter")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ----------------------------------------------")

    # 	Pick up the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, type=str, help="File to split, in FASTA format.")
    parser.add_argument("--partitions", required=True, type=int, help="Number of pieces to split the fasta file into.")
    parser.add_argument("--affix", required=False, type=str, help="Affix for Output files.")
    parser.add_argument("--version", required=False, type=str, help="Version number of reference database." )
    parser.add_argument("--out", required=False, type=str, help="Output Directory.")
    args = parser.parse_args()

    # 	Variables initialized from the command line arguments
    fastaFileToSplit     = args.file
    numberOfPartitons    = args.partitions
    affixForOutputFile   = args.affix if args.affix is not None else "partition"
    versionForOutputFile = args.version if args.affix is not None else "007"
    output_directory     = args.out if args.out is not None else "./out"


    # ------------------------------------------------- Output Files & Directories ----------------------------------------------
    #
    #   We'll check if the output directory exists - either the default (current) or the requested one
    #
    if not os.path.exists( output_directory ):
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" +
                " Output directory does not exist. Creating..." + "")

        output_directory = output_directory + "_" + str(numberOfPartitons)

        try:
            os.makedirs(output_directory)
        except OSError as e:
            if e.errno == 17:
                print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Output directory already exists...")


    # -------------------------------------------------------- Diagnostics ------------------------------------------------------

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] File to Split: " + fastaFileToSplit )
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] No. of Partitions: " + str(numberOfPartitons) + "")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Affix: " + affixForOutputFile)
    print( "[ " + time.strftime( '%d-%b-%Y %H:%M:%S', time.localtime() ) + " ] Version: " + versionForOutputFile )
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Output Dir: " + output_directory)
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")



    # -------------------------------------------------- FASTA File Preprocessing -----------------------------------------------
    #
    #   The splitting procedure works by using batch sizes to determine how many genomes will go into each of the number of
    #   requested partitions.  The user provides the number of partitions, and we have to determine what the batch size will be
    #   by counting the overall number of FASTA records in the file, and then dividing by the user-requested number of
    #   partitions.
    #
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Counting FASTA Records...")

    listOfRecordsInFastaFile = list(SeqIO.parse(fastaFileToSplit, "fasta"))

    totalNumberOfFASTARecords = len(listOfRecordsInFastaFile)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Total No. of Records: " +
          str("{:,}".format(totalNumberOfFASTARecords)) )

    batchSizeForPartitions = math.ceil(totalNumberOfFASTARecords / numberOfPartitons)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Batch Size: " + str(batchSizeForPartitions))
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")



    # ---------------------------------------------------- FASTA File Splitting -------------------------------------------------
    #
    #   The main idea here is to use an iterator function that will prevent the loading of the large file into memory.  By using
    #   the iterator function 'batch_iterator()' we are processing the large file in manageable batches.
    #
    print( "[ " + time.strftime( '%d-%b-%Y %H:%M:%S', time.localtime() ) + " ] Starting Split..." )

    sizeOfFastaFile = os.path.getsize( fastaFileToSplit )
    print( "[ " + time.strftime( '%d-%b-%Y %H:%M:%S', time.localtime() ) + " ] Size of FASTA file: " + str(sizeOfFastaFile) )


    #
    #   Split the fasta file using the batch iterator
    #
    record_iter = SeqIO.parse( open( fastaFileToSplit ), "fasta" )

    for i, batch in enumerate(batch_iterator(record_iter, batchSizeForPartitions)):

        output_directory_for_partition = output_directory + "/" + "%i" % (i + 1)

        if not os.path.exists(output_directory_for_partition):
            try:
                os.makedirs(output_directory_for_partition)
            except OSError as e:
                if e.errno == 17:
                    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Output Subdirectory already exists")

        outputFilename = output_directory_for_partition + "/" + affixForOutputFile + "_v" + versionForOutputFile + ".fasta"

        with open( outputFilename, "w" ) as handle:
            count = SeqIO.write( batch, handle, "fasta" )

        print( "Wrote %i records to %s" % (count, outputFilename) )



# ------------------------------------------------------------- End of Line -----------------------------------------------------

print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]")
print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Fasta File Splitter, Done.")
print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]")
sys.exit(0)
