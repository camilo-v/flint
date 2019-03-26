#!/usr/local/bin/python

# ---------------------------------------------------------------------------------------------------------------------
#
#                                              Bioinformatics Research Group
#                                                http://biorg.cis.fiu.edu/
#                                             Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	    The purpose of this program is to split a tab-5 formatted file of DNA sequencing reads into N-number
#       of shards.
#
#   NOTES:
#       Please see the dependencies section below for the required libraries (if any).  This script uses code,
#       and the general algorithm for splitting from
#       "https://stackoverflow.com/questions/43123142/how-to-print-the-first-n-lines-of-file".
#
#   DEPENDENCIES:
#       None.
#
#       You can check the python modules currently installed in your system by running: python -c "help('modules')"
#
#   USAGE:
#       Run the program with the "--help" flag to see usage instructions.
#
#	AUTHOR:
#           Camilo Valdes
#           cvalde03@fiu.edu
#           https://github.com/camilo-v
#
# ---------------------------------------------------------------------------------------------------------------------

# 	Python Modules
import os, sys
import time
import csv

# ------------------------------------------------------- Init --------------------------------------------------------
#
#   Start
#
if __name__ == "__main__":

    sys.stdout.flush()
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',
                               time.localtime()) + " ] ----------------------------------------------")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] TAB5 File Splitter")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',
                               time.localtime()) + " ] ----------------------------------------------")

    #
    #   Sample particulars.
    #
    base_dir    = "/path/to/base/directory/"
    sample_id   = "sample_id"
    file_tab5   = base_dir + "/" + sample_id + "/" + sample_id + "-tab5.txt"

    #
    #   Output
    #
    output_dir  = base_dir + "/" + sample_id + "/shards"
    if not os.path.exists(output_dir):
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',
                                   time.localtime()) + " ]" + " Output directory does not exist. Creating..." + "")
        os.makedirs(output_dir)

    #
    #   The limits (shard size) we'll be testing.
    #
    limits = [525000, 550000, 575000, 600000, 625000, 650000, 675000]

    #
    #   Read the first n-lines of the input file and write them out to their own shard file.
    #
    for a_limit in limits:
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Processing " + str(a_limit)
              + " reads...")

        output_file = output_dir + "/" + sample_id + "-" + str(a_limit) + "-tab5.txt"

        with open(file_tab5, 'r') as input_file:
            with open(output_file, 'w') as writer:
                try:
                    for line_number, line in enumerate(input_file):
                        if line_number > a_limit:
                            break
                        writer.write(line)

                except csv.Error as e:
                    sys.exit("[ERROR] " + str(e))


# ---------------------------------------------------- End of Line ----------------------------------------------------

print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" + " Done.")
print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
sys.exit(0)
