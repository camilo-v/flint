# coding: utf-8
# ---------------------------------------------------------------------------------------------------------------------
#
#                                       Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this file is to implement helper functions and miscellaneous utility methods that are used by Flint.
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
import os, sys
import time
import multiprocessing

__version__ = "RC1"
__build_number__ = "XR51972"

# --------------------------------------------------- Functions -------------------------------------------------------

def get_number_of_cpus() :

    """Gets the number of CPUs that the current machine has available.

    :returns: The number of CPUs (int) that are available to use
    """

    numberOfCPUs = multiprocessing.cpu_count()

    return numberOfCPUs



def get_number_of_lines_in_file(fileName):

    """Gets the number of lines in a file.

    :param fileName: The file name for which the number of lines will be calculated.
    :returns: The number of lines (int) in the file.
    """

    with open(fileName) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def getFlintVersion():
    """Returns the current version number of Flint.
    """
    return __version__


def getFlintBuildNumber():
    """Returns the current build number of Flint.
    """
    return __build_number__


def printFlintPrettyHeader():

    """"Prints to STD_Out a pretty header for logging.
    """

    sys.stdout.flush()
    print("--------------------------------------------------------------------")
    print(" Flint")
    print(" version " + getFlintVersion() + ", " + getFlintBuildNumber() )
    print("--------------------------------------------------------------------")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]")








