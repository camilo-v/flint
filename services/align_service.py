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
#	The purpose of this file is to implement a persistent subprocess that will maintain Bowtie2 running so that
#   it is available for alignment purposes without having to load the reference Index over an over again.
#
#
#   NOTES:
#   Please see the dependencies section below for the required libraries (if any).
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
import sys
import shlex
from subprocess import Popen, PIPE
import signal


# -------------------------------------------------------- Main -------------------------------------------------------
#
#
def main(args):

    #   Obtain a properly formatted Bowtie2 command.
    bowtieCMD = getBowtie2Command()

    try:
        #   Open a pipe to the subprocess that will launch the Bowtie2 aligner.
        align_subprocess = Popen(bowtieCMD, stdin=PIPE, stdout=sys.stdout, stderr=PIPE)

        #   Dispatch the subprocess using STDIN as the input.
        #   The Alignment Rate reported from Bowtie2 comes in through STDERR.
        alignment_output, alignment_error = align_subprocess.communicate(input=sys.stdin.read())


    except OSError as e:
        print("OS ERROR: " + str(e))



# ----------------------------------------------- Helper Functions -----------------------------------------------------
#
#
def getBowtie2Command():
    """
    Constructs a properly formatted shell Bowtie2 command by performing a simple lexical analysis using 'shlex.split()'.
    Returns:
        An array with the bowtie 2 command call split into an array that can be used by the popen() function.
    """

    index_location = '/mnt/bio_data/index/ensembl_v40'

    bowtieCMD = '/home/hadoop/apps/bowtie2-2.3.4.1-linux-x86_64/bowtie2 \
                                    --threads 6 \
                                    --local \
                                    -D 5 \
                                    -R 1 \
                                    -N 0 \
                                    -L 25 \
                                    -i \'"S,0,2.75"\' \
                                    --no-discordant \
                                    --no-mixed \
                                    --no-contain \
                                    --no-overlap \
                                    --no-sq \
                                    --no-hd \
                                    --no-unal \
                                    -q \
                                    -x ' + index_location + ' --tab5 -'

    return shlex.split(bowtieCMD)




def validate_output(stringToValidate):
    """
    Function for determining if the 'end' of the output has been reached, for 'some' definition of 'end'. :)
    Args:
        stringToValidate:   The string that we'll be checking for the special flag that tells us 'this is the end'.

    Returns:
        True:   If the output string contains the flag.
        False:  If the output string is empty or if it does not contain the flag.
    """

    sentinelString = 'overall alignment rate'

    if stringToValidate == '':
        return False
    elif sentinelString in stringToValidate:
        return True
    else:
        return False



def default_sigpipe():
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)



# ----------------------------------------------------------- Init ----------------------------------------------------
#
#   App Initializer.
#
if __name__ == "__main__":
    main(sys.argv[1:])
