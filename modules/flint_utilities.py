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

__VERSION__ = "RC3"
__BUILD_NUMBER__ = "B20190715"

# --------------------------------------------------- Functions -------------------------------------------------------

def get_number_of_cpus() :

    """
    Gets the number of CPUs that the current machine has available.
    :returns: The number of CPUs (int) that are available to use
    """

    numberOfCPUs = multiprocessing.cpu_count()

    return numberOfCPUs



def get_number_of_lines_in_file(fileName):

    """
    Gets the number of lines in a file.
    :param fileName: The file name for which the number of lines will be calculated.
    :returns: The number of lines (int) in the file.
    """

    with open(fileName) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def getFlintVersion():
    """
    Returns the current version number of Flint.
    """
    return __VERSION__


def getFlintBuildNumber():
    """
    Returns the current build number of Flint.
    """
    return __BUILD_NUMBER__


def printFlintPrettyHeader():
    """"
        Prints a nice-looking header for displaying in a Terminal.
        Flint header made with MonoDraw on macOS.
        https://monodraw.helftone.com
        :)
    """

    sys.stdout.flush()

    print(" ───────────────────────────────────────────────────────────────────────────────")
    print("    _______        _____ __   _ _______            Version " + getFlintVersion() + "." + getFlintBuildNumber())
    print("    |______ |        |   | \  |    |                       BioRG");
    print("    |       |_____ __|__ |  \_|    |                School of Computing")
    print("                                                 and Information Sciences")
    print("    https://github.com/camilo-v/flint                       FIU")
    print(" ───────────────────────────────────────────────────────────────────────────────")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + "]")








