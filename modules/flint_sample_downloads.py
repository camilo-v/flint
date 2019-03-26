# coding: utf-8

# ---------------------------------------------------------------------------------------------------------------------
#
#                                       Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	    The purpose of this file is to implement the sample download functions that are used by Flint.
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
import urllib
import ftplib
import ftputil
import fnmatch
import tarfile
import bz2

# ----------------------------------------------- Download Functions --------------------------------------------------


def download_sample(baseOutputDirectory, sampleID, urlToDownloadFrom, sampleType):
    
    """Downloads a sample from a specified URL.

    :param baseOutputDirectory: The output directory at which to download the sample files into.
    :param sampleID: The sampleID (prefix) for the sample that will be downloaded.
    :param urlToDownloadFrom: The URL in which the samples are stored in (FTP).
    :param sampleType: The type of sample (fastq or bam) to download.
    :returns: String with the path of the downloaded samples.
    """
    
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ] Downloading from " + urlToDownloadFrom + "")
    
    if sampleType == "fastq":   # URL base directory is 'Illumina'
        
        sys.stdout.flush()
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " FASTQ Request...")
        
        # -------------------------------------------- Output Dir -----------------------------------------------
        
        outputDirectoryForSample = baseOutputDirectory
        if not os.path.exists(outputDirectoryForSample):
            os.makedirs(outputDirectoryForSample)
        
        # ----------------------------------------------- FTP ---------------------------------------------------
        
        #   The sample is a directory — it contains paired end reads
        filenameForCompressedFASTQ        = sampleID + '.tar.bz2'
        localFilePathForCompressedFASTQ   = os.path.join(outputDirectoryForSample, filenameForCompressedFASTQ)
        
        #   We have to search for the sample in the remote site as we don't have a specific URL to fetch from
        with ftputil.FTPHost(urlToDownloadFrom, "anonymous","") as host:
            recursive = host.walk("/" + "Illumina", topdown=True, onerror=None)
            
            for root, dirs, files in recursive:                
                for name in files:
                    sampleList = fnmatch.filter(files, filenameForCompressedFASTQ)
                    
                    #   We need to iterate over sampleList constructing one pathname at a time, passing each 
                    #   of those pathnames to host.path.isfile()
                    for fname in sampleList:
                        fpath = host.path.join(root, fname)
                        if host.path.isfile(fpath):
                            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Downloading...")
                            host.download(fpath, localFilePathForCompressedFASTQ)
                            
                            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Extracting...")
                            tarFASTQ = tarfile.open(localFilePathForCompressedFASTQ, "r:bz2")
                            tarFASTQ.extractall(path=outputDirectoryForSample)
                            tarFASTQ.close()
                            
                            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Cleaning up...")
                            os.remove(localFilePathForCompressedFASTQ)
                            
                            #   Return the path to the extracted parent directory for this sample
                            return localFilePathForCompressedFASTQ[:-8]
        
        
    elif sampleType == "bam": # URL base directory is 'HMSCP'
        
        sys.stdout.flush()
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " BAM Request...")
        
        # -------------------------------------------- Output Dir -----------------------------------------------
        
        outputDirectoryForSample = baseOutputDirectory + "/" + sampleID
        if not os.path.exists( outputDirectoryForSample ):
            os.makedirs( outputDirectoryForSample )
        
        # ----------------------------------------------- FTP ---------------------------------------------------
        
        ftp = ftplib.FTP(urlToDownloadFrom)
        ftp.login()
        
        #   There are three (3) files we need to download (BAM, Metric, and Abundance)
        filenameForBAM          = sampleID + '_vs_RefDb.sorted.bam'
        filenameForMetric       = sampleID + '_metric.txt.bz2'
        filenameForAbundance    = sampleID + '_abundance_table.tsv.bz2'
    
        localFilePathForBAM     = outputDirectoryForSample + "/" + filenameForBAM
        localFilePathForMetric  = outputDirectoryForSample + "/" + filenameForMetric
        localFilePathForAbund   = outputDirectoryForSample + "/" + filenameForAbundance
    
        localFileTargetBAM      = open( localFilePathForBAM,'wb')
        localFileTargetMetric   = open( localFilePathForMetric,'wb')
        localFileTargetAbund    = open( localFilePathForAbund,'wb')
    
        remoteFileTargetBAM     = "HMSCP" + "/" + sampleID + "/" + filenameForBAM
        remoteFileTargetMetric  = "HMSCP" + "/" + sampleID + "/" + filenameForMetric
        remoteFileTargetAbund   = "HMSCP" + "/" + sampleID + "/" + filenameForAbundance
    
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Downloading...")
        ftp.retrbinary('RETR %s' % remoteFileTargetBAM, localFileTargetBAM.write)
        ftp.retrbinary('RETR %s' % remoteFileTargetMetric, localFileTargetMetric.write)
        ftp.retrbinary('RETR %s' % remoteFileTargetAbund, localFileTargetAbund.write)
    
        localFileTargetBAM.close()
        localFileTargetMetric.close()
        localFileTargetAbund.close()
    
        #   Once the files are downloaded, we'll need to decompress the Metric and Abundance files
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Extracting Metric files...")
        compressedMetricFile    = bz2.BZ2File(localFilePathForMetric)
        dataForMetricFile       = compressedMetricFile.read()
        open(localFilePathForMetric[:-4], 'wb').write(dataForMetricFile)
        os.remove(localFilePathForMetric)
    
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S',time.localtime()) + " ]" + " Extracting Abundance files...")
        compressedAbundanceFile = bz2.BZ2File(localFilePathForAbund)
        dataForAbundanceFile    = compressedAbundanceFile.read()
        open(localFilePathForAbund[:-4], 'wb').write(dataForAbundanceFile)
        os.remove(localFilePathForAbund)
    
        return localFilePathForBAM


