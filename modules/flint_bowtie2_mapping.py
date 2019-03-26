# coding: utf-8

# ---------------------------------------------------------------------------------------------------------------------
#
#                                       Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	    The purpose of this module is to implement the Bowtie2 mapping step. In this step we'll be mapping a FASTQ
#       sample against an index (created with bowtie2-build).
#
#
#   NOTES:
#   Please see the dependencies section below for the required libraries (if any).
#
#   DEPENDENCIES:
#
#       • Python & the modules listed below
#       • Bowtie2
#       • SamTools
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


# -------------------------------------------- Bowtie2 Mapping Functions ----------------------------------------------

def mapSampleWithBowtie(sampleFASTQFile):

    """Maps a sample (fastq) against a Bowtie2 index.

    :param sampleFASTQFile: The sample that will be mapped against the index, in FASTQ format.
    :returns: String with the path of the BAM file of alignments for sampleFASTQFile.
    """

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]  Mapping Sample...")
