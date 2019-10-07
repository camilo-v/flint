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
import operator

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
    parser.add_argument("--sensitive", action="store_true", required=False, help="Sensitive Alignment Mode.")
    parser.add_argument("--verbose", action="store_true", required=False, help="Wordy Terminal output.")
    parser.add_argument("--report_all", action="store_true", required=False,
                        help="Report all bacterial abundances, including those with 0 count. " +
                        "Works only with the '--coalesce_output' flag.")
    parser.add_argument("--keep_shard_profiles", action="store_true", required=False,
                        help="Retain individual shard profiles, and store them in S3 or the local filesystem.")
    parser.add_argument("--coalesce_output", action="store_true", required=False,
                        help="Output will be merged into a single file for abundance reports.")
    parser.add_argument("--debug", action="store_true", required=False, help="Debug mode. VERY SLOW.")
    parser.add_argument("--timeout", type=int, default=3,
                        help="Elapsed time at which streaming will stop after not retrieving any data.")
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument('--output_s3', action='store_true', help="Save output to AWS S3 bucket.")
    output_group.add_argument('--output_local', action='store_true', help="Save output to local filesystem.")
    streaming_group = parser.add_mutually_exclusive_group()
    streaming_group.add_argument("--stream_dir", action="store_true", required=False,
                                 help="Stream from a directory source.")
    streaming_group.add_argument("--stream_kinesis", action="store_true", required=False,
                                 help="Stream from a Kinesis source.")

    #   Grab the arguments that were sent either through the command line, or through a module main() call.
    args = parser.parse_args(args)

    # 	The configuration JSON file contains information about the samples we'll be processing. This also includes
    #   information about Bowtie2, the Bowtie2 index, and the size of the cluster.
    #
    filePathForInputJSONFile = args.samples
    filePathForInputJSONFile.strip()

    #   Grab the remaining arguments
    verbose_output          = args.verbose
    use_streaming_dir       = args.stream_dir
    use_streaming_kinesis   = args.stream_kinesis
    save_to_s3              = args.output_s3
    save_to_local           = args.output_local
    sensitive_align         = args.sensitive
    keep_shard_profiles     = args.keep_shard_profiles
    report_all              = args.report_all
    coalesce_output         = args.coalesce_output
    debug_mode              = args.debug
    streaming_timeout       = args.timeout

    # ----------------------------------------------- Run Configuration -----------------------------------------------
    #
    #   Load the configuration JSON file with the sample particulars.
    #
    with open( filePathForInputJSONFile ) as sampleDataFile:
        sampleData = json.load(sampleDataFile)

    if debug_mode:
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] DEBUG MODE: [0N] ⚠️")

    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Loading Run parameters...")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Streaming Timeout: " +
          str(streaming_timeout) + " seconds.")

    try:
        arrayOfSamples      = sampleData["samples"]
        partition_size      = sampleData["partition_size"]
        partition_size      = int(partition_size)
        bowtie2_path        = sampleData["bowtie2_path"]
        bowtie2_index_path  = sampleData["bowtie2_index_path"]
        bowtie2_index_name  = sampleData["bowtie2_index_name"]
        annotations         = sampleData["annotations"]

    except KeyError as run_param_error:
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] WARNING! Run Configuration Error. " +
              "Missing :" + str(run_param_error))

    #   Set the parameters we need for Bowtie2
    if verbose_output:
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Bowtie2 Path: " + str(bowtie2_path))

    default_bowtie2_threads = 6

    if "bowtie2_threads" in sampleData:
        bowtie2_threads = sampleData["bowtie2_threads"]
    else:
        bowtie2_threads = default_bowtie2_threads

    sj.set_bowtie2_path(bowtie2_path)
    sj.set_bowtie2_index_path(bowtie2_index_path)
    sj.set_bowtie2_index_name(bowtie2_index_name)
    sj.set_bowtie2_number_threads(bowtie2_threads)


    # --------------------------------------------- Annotations Parsing -----------------------------------------------
    #
    #   Annotations are used to create output reports.
    #
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Loading Annotations...")

    #
    #   Will 'map' a taxonomic id (GCA_) to a full organism name at the Strain level.
    #
    annotations_dictionary = {}
    number_of_strains_in_annotations = 0

    try:
        annotations_bucket  = annotations["bucket"]
        annotations_path    = annotations["path"]

    except KeyError as annot_key_error:
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] WARNING! Annotations Error. " +
              str(annot_key_error))

    client = boto3.client('s3')
    s3_obj = client.get_object(Bucket=annotations_bucket, Key=annotations_path)
    annotations_file = s3_obj['Body'].read()
    annotations_df = pd.read_csv(io.BytesIO(annotations_file), header=None, delimiter="\t")

    for index, row in annotations_df.iterrows():
        taxonomic_id  = row[0]
        gca_id        = row[2].split('.')[0]
        organism_name = row[1]

        annotations_dictionary[gca_id] = {'taxa_id': taxonomic_id, 'organism_name': organism_name}

        number_of_strains_in_annotations += 1

    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] " + "Organisms in Annotations: " +
          '{:0,.0f}'.format(number_of_strains_in_annotations))


    # --------------------------------------------- Sample Processing -------------------------------------------------
    #
    #
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Getting Sample Particulars...")

    for aSample in arrayOfSamples:

        #   At a minimum, for each sample we need an ID and a sample type. The other properties are base on whether
        #   this is a streaming job or not.
        try:
            sampleID        = aSample['id']
            sample_format   = aSample['sample_format']
            sample_type     = aSample['sample_type']

        except KeyError as sample_requirements_key_error:
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] [ERROR] ⚠️ Sample Requirements Error." + str(sample_requirements_key_error))
            exit(1)

        try:
            if sample_format.lower() != "tab5":
                raise ValueError()
        except (ValueError, IndexError):
            print("] [ERROR] ⚠️ Read Format Error. Only TAB5-formatted read files are supported.")
            exit(1)


        try:
            if sample_type.lower() not in ("single", "paired"):
                raise ValueError()
        except (ValueError, IndexError):
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] [ERROR] ⚠️ Read Type Error. Only Paired or Single reads supported.")
            exit(1)


        # --------------------------------- Properties for Streaming from a Directory ---------------------------------
        if use_streaming_dir:
            try:
                batch_duration      = float(aSample["batch_duration"])  # In seconds.
                app_name            = aSample["streaming_app_name"]
                output_directory    = aSample["output_dir"]
                stream_source_dir   = aSample["stream_dir"]
                number_of_shards    = aSample["number_of_shards"]

            except KeyError as stream_dir_key_error:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] [ERROR] ⚠️ Stream Directory Key Error. Missing: " + str(stream_dir_key_error))
                exit(1)

        # ----------------------------------- Properties for Streaming from Kinesis -----------------------------------
        elif use_streaming_kinesis:
            try:
                batch_duration      = float(aSample["batch_duration"])  # In seconds.
                app_name            = aSample["streaming_app_name"]
                output_directory    = aSample["output_dir"]
                stream_name         = aSample["stream_name"]
                endpoint_url        = aSample["endpoint_url"]
                region_name         = aSample["region_name"]
                number_of_shards    = aSample["number_of_shards"]

            except KeyError as stream_kinesis_key_error:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] [ERROR] ⚠️ Stream Kinesis Key Error. Missing: " + str(stream_kinesis_key_error))
                exit(1)


        # ------------------------------ Properties for local processing (non-streaming) ------------------------------
        else:
            try:
                mate_1 = aSample["mate_1"]
                mate_2 = aSample["mate_2"]
            except KeyError as non_stream_key_error:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] [ERROR] ⚠️ Non-Streaming Key Error. Missing: " +
                      str(non_stream_key_error))
                exit(1)


        # ---------------------------------------------- Output Files -------------------------------------------------
        #
        # If we are not saving to AWS S3, then we'll check if the output directory exists — either the default
        # (current) or the requested one

        output_file = output_directory + "/" + sampleID
        output_file_name = 'abundances.txt'

        if verbose_output:
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Save to Amazon S3: " +
                  str(save_to_s3) )
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Save to Local Filesystem: "
                  + str(save_to_local))

        if keep_shard_profiles:
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] Retaining Individual Shard Profiles.")

        s3_output_bucket = ""
        if save_to_s3:
            s3_output_bucket = aSample['output_bucket']
            output_file = output_file + "/" + output_file_name

        if save_to_local:
            local_output_directory = output_directory + "/" + sampleID
            if not os.path.exists(local_output_directory):
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]" +
                      " Output directory does not exist. Creating...")
                os.makedirs(local_output_directory)

            if keep_shard_profiles:
                local_shard_profile_output_dir = local_output_directory + "/shard_profiles"
                if not os.path.exists(local_shard_profile_output_dir):
                    os.makedirs(local_shard_profile_output_dir)

            output_file = local_output_directory + "/" + output_file_name


        # -------------------------------------------------- Spark ----------------------------------------------------
        #
        #   The following sets the final, and trivial, configurations for the Spark run. Do not set any other flags
        #   here, as they are ignored in EMR. To set anything, do so with "--conf" in the "spark-submit" call.
        #

        #   Name for label that appears in EMR and Spark dashboard output.
        APP_NAME = "Flint - " + str(sampleID)

        #   Configuration parameters for a Spark run in an EMR cluster.
        conf = (SparkConf().setAppName(APP_NAME))
        conf.set("spark.default.parallelism", partition_size)
        conf.set("spark.executor.memoryOverhead", "1G")
        conf.set("conf spark.locality.wait", "3s")
        conf.set("spark.network.timeout", "10000000")
        conf.set("spark.executor.heartbeatInterval", "10000000")


        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Batch Duration: " +
              str(batch_duration))
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Configuring Spark...")

        #   Initialize the Spark context for this run.
        sc = SparkContext(conf=conf)

        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/spark_jobs.py'))
        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/flint_sample_downloads.py'))
        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/flint_utilities.py'))
        sc.addPyFile(os.path.join(os.path.dirname(__file__), 'modules/flint_bowtie2_mapping.py'))

        #   Add the DNA mapping resources
        sc.addFile(os.path.join(os.path.dirname(__file__), 'services/align_service.py'))

        #   Initialize the Spark Streaming context, this is the main entry point of all Spark Streaming
        #   functionality.
        ssc = StreamingContext(sc, batch_duration)

        # -------------------------------------- Stream from a Directory ----------------------------------------------
        if use_streaming_dir:
            try:
                sj.dispatch_stream_from_dir(stream_source_dir=stream_source_dir,
                                            sampleID=sampleID,
                                            sample_format=sample_format,
                                            output_file=output_file,
                                            save_to_s3=save_to_s3,
                                            save_to_local=save_to_local,
                                            partition_size=partition_size,
                                            ssc=ssc,
                                            sensitive_align=sensitive_align,
                                            annotations_dictionary=annotations_dictionary,
                                            s3_output_bucket=s3_output_bucket,
                                            verbose_output=verbose_output,
                                            number_of_shards=number_of_shards,
                                            streaming_timeout=streaming_timeout,
                                            keep_shard_profiles=keep_shard_profiles,
                                            coalesce_output=coalesce_output,
                                            sample_type=sample_type,
                                            debug_mode=debug_mode
                                            )
            except ValueError, e:
                print(str(e))


        # ---------------------------------- Stream from a Kinesis source ---------------------------------------------
        if use_streaming_kinesis:
            try:
                sj.dispatch_stream_from_kinesis(sampleID=sampleID,
                                                sample_format=sample_format,
                                                output_file=output_file,
                                                save_to_s3=save_to_s3,
                                                save_to_local=save_to_local,
                                                partition_size=partition_size,
                                                ssc=ssc,
                                                app_name=app_name,
                                                stream_name=stream_name,
                                                endpoint_url=endpoint_url,
                                                region_name=region_name,
                                                number_of_shards=number_of_shards,
                                                streaming_timeout=streaming_timeout,
                                                sensitive_align=sensitive_align,
                                                annotations_dictionary=annotations_dictionary,
                                                s3_output_bucket=s3_output_bucket,
                                                verbose_output=verbose_output,
                                                keep_shard_profiles=keep_shard_profiles,
                                                coalesce_output=coalesce_output,
                                                sample_type=sample_type,
                                                debug_mode=debug_mode
                                                )
            except ValueError, e:
                print(str(e))


        # -------------------------------------- Coalesced Output Reports ---------------------------------------------
        #
        #   Reports are written out to either an S3 bucket specified in the initial JSON config file, or to a
        #   local path in the local filesystem. Note that we map the abundances so that we get a nice
        #   tab-delimited file, then repartition it so that we only get a single file, and not multiple ones for
        #   each partition.
        #
        if coalesce_output:
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] Writing Coalesced Output Reports...")

            #
            #   Sort the overall abundances so that we can report descending values, or most prominent Strains at top.
            #   The call to 'sorted()' will return a sorted list of tuples, so if we want the key ('gca_id'), we'll
            #   need to address the first value in the object, i.e., 'gca_id = sorted_tuple[0]'.
            #
            overall_abundances = sj.get_overall_abundaces()
            sorted_overall_abundances = sorted(overall_abundances.value.items(),
                                               key=operator.itemgetter(1),
                                               reverse=True)

            seen_strains = {}   #   Maps a strain's GCA_ID (KEY) to a flag of whether we saw it in the sample.
            output_list  = []   #   Contains the data that we'll be writing out.

            for sorted_tuple in sorted_overall_abundances:
                gca_id = sorted_tuple[0]
                if gca_id in annotations_dictionary:
                    taxa_id       = annotations_dictionary[gca_id]['taxa_id']
                    organism_name = annotations_dictionary[gca_id]['organism_name']
                    output_list.append([str(taxa_id),
                                        str(gca_id),
                                        str(organism_name),
                                        "{0:.4f}".format(sj.OVERALL_ABUNDANCES.value[gca_id])])
                    seen_strains[gca_id] = 1
                else:
                    output_list.append([gca_id, sj.OVERALL_ABUNDANCES.value[gca_id]])

            if report_all:
                for gca_id in annotations_dictionary:
                    if gca_id in seen_strains:
                        continue
                    else:
                        taxa_id = annotations_dictionary[gca_id]['taxa_id']
                        organism_name = annotations_dictionary[gca_id]['organism_name']
                        output_list.append([str(taxa_id),
                                            str(gca_id),
                                            str(organism_name),
                                            "0.000000"])


            # ------------------------------------------ Local Output -------------------------------------------------
            #
            #   Save a coalesced 'abundances.txt' output file to the 'local' filesystem of the Master Node.
            #
            if save_to_local:
                if verbose_output:
                    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                          "] Saving to local filesystem...")

                writer = csv.writer(open(output_file, "wb"), delimiter='\t', lineterminator="\n", quotechar='',
                                    quoting=csv.QUOTE_NONE)

                for a_line in output_list:
                    writer.writerow(a_line)

            # -------------------------------------------- S3 Output --------------------------------------------------
            #
            #   Save a coalesced 'abundances.txt' output file to the S3 bucket specified.
            #
            if save_to_s3:
                if verbose_output:
                    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                          "] Saving to S3 bucket...")

                output_df  = pd.DataFrame(output_list)
                csv_buffer = io.BytesIO()
                output_df.to_csv(csv_buffer, header=False, sep="\t", index=False)

                response = client.put_object(
                    Bucket=s3_output_bucket,
                    Body=csv_buffer.getvalue(),
                    Key=output_file
                )


        # ------------------------------------------------ Wrap-Up ----------------------------------------------------
        start_time  = sj.ANALYSIS_START_TIME
        end_time    = sj.ANALYSIS_END_TIME
        run_time    = end_time - start_time

        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Analysis Run Time: " +
                str(timedelta(seconds=run_time)))
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Complete.")
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]")


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
