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
#	    The purpose of this program is to create the primary Spark driver application for the implementation of the
#       Flint metagenomic profiling and analysis framework.  This script contains the the application's "main()"
#       function and will define the data structures to be run in the cluster.
#
#
#   NOTES:
#   Please see the dependencies section below for the required libraries (if any).
#
#   DEPENDENCIES:
#       • Apache-Spark
#       • Python
#       • Biopython
#       • Boto3
#       • Fabric
#       • Pandas
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
# ---------------------------------------------------------------------------------------------------------------------

#   Spark Modules
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

# 	Python Modules
import io, os, sys
import argparse
import time
import json
import csv
import boto3
import pandas as pd
from datetime import timedelta

#   Flint Modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))
import flint_utilities as utils
import spark_jobs as sj
import flint_bowtie2_mapping as bowtieUtils


# -------------------------------------------------------- Main -------------------------------------------------------
#
#
def main(args):
    """
    Main function of the app.
    Args:
        args: command line arguments.

    Returns:

    """
    utils.printFlintPrettyHeader()

    # 	Pick up the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", required=True, type=str, help="Manifest JSON file with sample particulars.")
    parser.add_argument("--affix", required=False, type=str, help="Affix for Output files.")
    parser.add_argument("--stream", action="store_true", required=False, help="Realtime Stream Processing.")
    parser.add_argument("--s3_output", action="store_true", required=False, help="Save output to AWS S3.")
    parser.add_argument("--sensitive", action="store_true", required=False, help="Sensitive Alignment Mode.")

    #   Grab the arguments that were sent either through the command line, or through a module main() call.
    args = parser.parse_args(args)

    # 	The input JSON file contains information about the samples we'll be processing. This includes the base URL (path)
    #   at which we can find the samples, as well as the sample types (bam, fastq are supported).
    filePathForInputJSONFile = args.samples
    filePathForInputJSONFile.strip()

    #   Grab the remaining arguments
    affixForOutputFile = args.affix if args.affix is not None else "default_out"
    use_streaming = args.stream
    save_to_s3 = args.s3_output
    sensitive_align = args.sensitive

    # ------------------------------------------------ Sample Loading -------------------------------------------------
    #

    #   Load the samples JSON file with the sample particulars
    with open( filePathForInputJSONFile ) as sampleDataFile:
        sampleData = json.load(sampleDataFile)

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Loading run parameters...")

    baseURL         = sampleData["base_url"]
    arrayOfSamples  = sampleData["samples"]
    partition_size  = sampleData["partition_size"]
    partition_size  = int(partition_size)
    annotations     = sampleData["annotations"]

    # --------------------------------------------- Annotations Parsing -----------------------------------------------
    #
    #   Annotations are used to create output reports.
    #
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Loading Annotations...")

    #
    #   Will 'map' a taxonomic id (GCA_) to a full organism name at the Strain level.
    #
    annotations_dictionary = {}
    number_of_strains_in_annotations = 0

    client = boto3.client('s3')
    s3_obj = client.get_object(Bucket='your-bucket-name', Key='ensembl/41/annotations/annotations_ensembl_v41.txt')
    annotations_file = s3_obj['Body'].read()
    annotations_df = pd.read_csv(io.BytesIO(annotations_file), header=None, delimiter="\t")

    for index, row in annotations_df.iterrows():
        taxonomic_id  = row[0]
        gca_id        = row[2].split('.')[0]
        organism_name = row[1]

        annotations_dictionary[gca_id] = {'taxa_id': taxonomic_id, 'organism_name': organism_name}

        number_of_strains_in_annotations += 1

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] " + "Organisms in Annotations: " +
          '{:0,.0f}'.format(number_of_strains_in_annotations))


    # --------------------------------------------- Sample Processing -------------------------------------------------
    #
    #
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Getting sample particulars...")

    for aSample in arrayOfSamples:

        sampleID    = aSample['id']
        sampleType  = aSample['type']
        mate_1      = aSample["mate_1"]
        mate_2      = aSample["mate_2"]
        tab5        = aSample["tab5"]
        stream_source_dir  = aSample["stream_dir"]
        output_directory   = aSample["output_dir"]
        batch_duration      = float(aSample["batch_duration"])  # In seconds.
        app_name = aSample["app_name"]
        stream_name = aSample["stream_name"]
        endpoint_url = aSample["endpoint_url"]
        region_name = aSample["region_name"]

        # ---------------------------------------------- Output Files -------------------------------------------------
        #
        # If we are not saving to AWS S3, then we'll check if the output directory exists — either the default
        # (current) or the requested one

        output_file = output_directory + "/" + sampleID

        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Save to Amazon S3: " + str(save_to_s3) )

        if not save_to_s3:
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Output to local filesystem...")
            local_output_directory = "/mnt/bio_data/results/abundances" + "/" + sampleID
            if not os.path.exists(local_output_directory):
                print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" +
                      " Output directory does not exist. Creating...")
                os.makedirs(local_output_directory)

            output_file = local_output_directory + "/" + sampleID + "-abundances.txt"


        # -------------------------------------------------- Spark ----------------------------------------------------
        #
        #   The following sets the final, and trivial, configurations for the Spark run. Do not set any other flags
        #   here, as they are ignored in EMR. To set anything, do so with "--conf" in the "spark-submit" call.
        #

        APP_NAME = "Flint"
        conf = ( SparkConf().setAppName(APP_NAME) )

        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Configuring Spark...")

        #   Initialize the Spark context for this run.
        sc = SparkContext(conf=conf)

        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/spark_jobs.py'))
        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/flint_sample_downloads.py'))
        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/flint_utilities.py'))
        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/flint_bowtie2_mapping.py'))

        #   Add the DNA mapping resources
        sc.addFile(os.path.join(os.path.dirname(__file__), 'services/align_service.py'))
        sc.addFile(os.path.join(os.path.dirname(__file__), 'services/pipe_test.sh'))
        dnaMappingScript = "pipe_test.sh"

        start_time = time.time()

        if use_streaming:
            # ----------------------------------------- Spark Streaming------------------------------------------------
            ssc = StreamingContext(sc, batch_duration)

            try:
                sj.dispatchSparkStreamingJob(stream_source_dir,
                                             batch_duration,
                                             dnaMappingScript,
                                             sampleID,
                                             sampleType,
                                             output_file,
                                             save_to_s3,
                                             partition_size,
                                             ssc,
                                             app_name,
                                             stream_name,
                                             endpoint_url,
                                             region_name,
                                             annotations,
                                             output_directory,
                                             sensitive_align,
                                             annotations_dictionary)
            except ValueError, e:
                print(str(e))

        else:
            # ---------------------------------- Non-Stream (regular) Spark Job ---------------------------------------
            try:
                sj.dispatchSparkJob(mate_1,
                                    mate_2,
                                    tab5,
                                    dnaMappingScript,
                                    sampleID,
                                    sampleType,
                                    output_file,
                                    save_to_s3,
                                    partition_size,
                                    sc)
            except ValueError, e:
                print(str(e))

        end_time = time.time()
        run_time = end_time - start_time

        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Run Time: " +
                "" + str(timedelta(seconds=run_time)) + "")
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Done.")


        # ----------------------------------------------- Spark Stop --------------------------------------------------
        #
        #   Shut down the cluster once everything completes.
        sc.stop()



# ----------------------------------------------------------- Init ----------------------------------------------------
#
#   App Initializer.
#
if __name__ == "__main__":
    main(sys.argv[1:])



# -------------------------------------------------------- End of Line ------------------------------------------------
