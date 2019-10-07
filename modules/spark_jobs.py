# coding: utf-8
# ---------------------------------------------------------------------------------------------------------------------
#
#                                       Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this file is to contain the principal Spark job function that is dispatched after the main Flint
#   function has collected all the necessary files.#
#
#   NOTES:
#   Please see the dependencies section below for the required libraries (if any).
#
#   DEPENDENCIES:
#
#       • Biopython
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
from datetime import timedelta
import csv
import pprint as pp
from pathlib2 import Path
import shlex
import pickle
import json
import subprocess as sp
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.accumulators import AccumulatorParam



# ------------------------------------------------ Custom Classes -----------------------------------------------------
#
class AbundanceAccumulator(AccumulatorParam):
    """
    Custom class for stockpiling the rolling abundance counts for a given bacterial strain. Big thanks go to
    StackOverflow and the following post:
    https://stackoverflow.com/questions/44640184/accumulator-in-pyspark-with-dict-as-global-variable

    """
    def zero(self,  value = ""):
        return dict()

    def addInPlace(self, value1, value2):
        value1.update(value2)
        return value1



# ---------------------------------------------------- Global ---------------------------------------------------------
#
#   We know. Global vars are bad. The team is working very hard to refactor this, and we're hoping on removing them
#   in an update soon.
#

BOWTIE2_PATH = ""
BOWTIE2_INDEX_PATH = ""
BOWTIE2_INDEX_NAME = ""
BOWTIE2_THREADS = 2

RDD_COUNTER = 0
NUMBER_OF_SHARDS_ALL = 0
ANALYSIS_START_TIME = 0
ANALYSIS_END_TIME = 0

TIME_OF_LAST_RDD = 0

#
#   Assorted methods for accessing the above.
#
def increment_rdd_count():
    """
    Increments the count that we use as an affix for the profile files.
    Returns:
            Nothing. It just increments the counter variable that we use to keep track of the incoming RDDs.
    """
    global RDD_COUNTER
    RDD_COUNTER = RDD_COUNTER + 1


def set_number_of_shards(num_shards_from_run):
    """
    Sets global number of shards to the specified number of shards from an experimental run.
    Args:
        num_shards_from_run:    The number of shards that we'll be streaming in.
    Returns:
        Nothing. This is a 'set()' function.
    """
    global NUMBER_OF_SHARDS_ALL
    NUMBER_OF_SHARDS_ALL = num_shards_from_run


def get_shard_counter():
    """
    Accessor for returning the current number of shards that we have processed.
    Returns:
            Integer containing the current count of processed shards.
    """
    global RDD_COUNTER
    return int(RDD_COUNTER)


def get_shards():
    """
    Accessor for returning the value of the overall number of shards we want to analyze.
    Returns:
            Integer containing the number of overall shards.
    """
    global NUMBER_OF_SHARDS_ALL
    return int(NUMBER_OF_SHARDS_ALL)


def shard_equals_counter():
    """
    Checks whether the number of shards processed equals the specified limit.
    Returns:
            True if its time to stop, the shards processed equal the number specified.
            False otherwise.
    """
    equality_check = False
    if get_shard_counter() == get_shards():
        equality_check = True
    return equality_check

def set_analysis_start_time():
    """
    Sets the time for the analysis when the first streamed shard is captured.
    Returns:
            Nothing, this is a 'setter' method.
    """
    global ANALYSIS_START_TIME
    ANALYSIS_START_TIME = time.time()

def set_analysis_end_time():
    """
    Sets the time for when all the shards have been processed.
    Returns:
            Nothing, this is a 'setter' method.
    """
    global ANALYSIS_END_TIME
    ANALYSIS_END_TIME = time.time()

def set_bowtie2_path(bowtie2_node_path):
    """
    Sets the path at which the Bowtie2 executable can be located.
    Args:
        bowtie2_node_path:  A path in the local nodes at which Bowtie2 can be found at. Note that this should match
                            the path that was given in the "app-setup.sh" bootstrap script.

    Returns:
        Nothing. This is a setter method.
    """
    global BOWTIE2_PATH
    BOWTIE2_PATH = bowtie2_node_path

def get_bowtie2_path():
    """
    Retrieves the path at which the Bowtie2 executable can be called.

    Returns:
        A path in the local nodes at which Bowtie2 can be found at. Note that this will match the path that
        was given in the "app-setup.sh" bootstrap script.
    """
    global BOWTIE2_PATH
    return BOWTIE2_PATH

def set_bowtie2_index_path(bowtie2_index_path):
    """
    Sets the path at which the Bowtie2 index can be found in each local node in the cluster.
    Args:
        bowtie2_index_path: A path in the local nodes at which Bowtie2 can be found at. Note that this should match
                            the path that was given in the "app-setup.sh" bootstrap script.

    Returns:
        Nothing.
    """
    global BOWTIE2_INDEX_PATH
    BOWTIE2_INDEX_PATH = bowtie2_index_path

def get_bowtie2_index_path():
    """
    Retrieves the path at which the local Bowtie2 index can be found.
    Returns:
        A path in the local nodes at which Bowtie2 can be found at. Note that this will match the path that
        was given in the "app-setup.sh" bootstrap script.
    """
    global BOWTIE2_INDEX_PATH
    return BOWTIE2_INDEX_PATH

def set_bowtie2_index_name(bowtie2_index_name):
    """
    Sets the name of the Bowtie2 index name we'll be using.
    Returns:
        Nothing.
    """
    global BOWTIE2_INDEX_NAME
    BOWTIE2_INDEX_NAME = bowtie2_index_name

def get_bowtie2_index_name():
    """
    Retrieves the name of the Bowtie2 index name we'll be using.
    Returns:
        A string with the name of the Bowtie2 index.
    """
    global BOWTIE2_INDEX_NAME
    return BOWTIE2_INDEX_NAME

def set_bowtie2_number_threads(bowtie2_number_threads):
    """
    Sets the number of threads in a Bowtie2 command.
    Args:
        bowtie2_number_threads: INT, the number of threads to give to bowtie2.

    Returns:
        Nothing.
    """
    global BOWTIE2_THREADS
    BOWTIE2_THREADS = bowtie2_number_threads

def get_bowtie2_number_threads():
    """
    Get the number of threads that Bowtie2 is currently using.
    Returns:
        INT The number of bowtie2 threads.
    """
    global BOWTIE2_THREADS
    return BOWTIE2_THREADS

def set_time_of_last_rdd(time_of_last_rdd_processed):
    """
    Sets the time at which the last RDD was processed.
    Returns:
        Nothing. Set() method.
    """
    global TIME_OF_LAST_RDD
    TIME_OF_LAST_RDD = time_of_last_rdd_processed


def get_time_of_last_rdd():
    """
    Retrieves the time at which the last RDD was processed.
    Returns:
        TIME_OF_LAST_RDD object that represents the time at which the last RDD ended processing.
    """
    global TIME_OF_LAST_RDD
    return TIME_OF_LAST_RDD

#
#   Dictionary.
#   The 'OVERALL_ABUNDANCES' dictionary contains the rolling sum of the abundances from each of the
#   ingested shards. When there are no more shards to process, we write the abundances to a file.
#
OVERALL_ABUNDANCES = {}

def set_overall_abundances(abundance_acc):
    global OVERALL_ABUNDANCES
    OVERALL_ABUNDANCES = abundance_acc

def get_overall_abundaces():
    global OVERALL_ABUNDANCES
    return OVERALL_ABUNDANCES



# -------------------------------------------- Stream from a Directory ------------------------------------------------
#
#
def dispatch_stream_from_dir(stream_source_dir, sampleID, sample_format, output_file, save_to_local, save_to_s3,
                             partition_size, ssc, sensitive_align, annotations_dictionary, s3_output_bucket,
                             number_of_shards, keep_shard_profiles, coalesce_output, sample_type, verbose_output,
                             debug_mode, streaming_timeout):
    """
    Executes the requested Spark job in the cluster using a streaming strategy.
    Args:
        stream_source_dir:      The directory to stream files from.
        number_of_shards:       The number of shards that we'll be picking up from 'stream_source_dir'.
        keep_shard_profiles:    Retains the rolling shard profiles in S3 or the local filesystem.
        sampleID:               The unique id of the sample.
        sample_format:          What type of input format are the reads in (tab5, fastq, tab6, etc.).
        sample_type:            Are the reads single-end or paired-end.
        output_file:            The path to the output file.
        save_to_s3:             Flag for storing output to AWS S3.
        save_to_local:          Flag for storing output to the local filesystem.
        partition_size:         Level of parallelization for RDDs that are not partitioned by the system.
        ssc:                    Spark Streaming Context.
        sensitive_align:        Sensitive Alignment Mode.
        annotations_dict:       Dictionary of Annotations for reporting organism names.
        s3_output_bucket:       The S3 bucket to write files into.
        coalesce_output:        Merge output into a single file.
        verbose_output:         Flag for wordy terminal print statements.
        debug_mode:             Flag for debug mode. Activates slow checkpoints.
        streaming_timeout:      Time (in sec) after which streaming will stop.

    Returns:
        Nothing, if all goes well it should return cleanly.

    """

    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] ")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
          "] Stream Source: [DIRECTORY]")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Sample ID: " + sampleID +
          " (" + sample_format + ", " + sample_type + ")")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Number of Sample Shards: " +
          str(number_of_shards))
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Streaming starting...")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
          "] Please copy reads into streaming directory.")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Waiting for input...")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] ")

    #   Set the number of shards so that we can safely exit after we have analyzed the requested number of shards.
    set_number_of_shards(int(number_of_shards))

    kinesis_decode = False

    #   Tab5-formatted FASTQ reads.
    if sample_format == "tab5":

        #
        #   Before we do anything, we have to move the data back to the Master. The way Spark Streaming works, is that
        #   the RDDs will be processed in the Worker in which they were received, which does not parallelize well.
        #   If we did not ship the input reads back to the master, then they would only be aligned in one Executor.
        #
        sc = ssc.sparkContext

        overall_abundance_accumulator = sc.accumulator({}, AbundanceAccumulator())
        set_overall_abundances(overall_abundance_accumulator)

        #   In this approach, we'll stream the reads from a S3 directory that we monitor with Spark.
        sample_dstream = ssc.textFileStream(stream_source_dir)

        sample_dstream.foreachRDD(lambda rdd: profile_sample(sampleReadsRDD=rdd,
                                                             sc=sc,
                                                             ssc=ssc,
                                                             output_file=output_file,
                                                             save_to_s3=save_to_s3,
                                                             save_to_local=save_to_local,
                                                             sample_type=sample_type,
                                                             sensitive_align=sensitive_align,
                                                             annotations_dictionary=annotations_dictionary,
                                                             partition_size=partition_size,
                                                             s3_output_bucket=s3_output_bucket,
                                                             kinesis_decode=kinesis_decode,
                                                             keep_shard_profiles=keep_shard_profiles,
                                                             coalesce_output=coalesce_output,
                                                             verbose_output=verbose_output,
                                                             debug_mode=debug_mode,
                                                             streaming_timeout=streaming_timeout,
                                                             bowtie2_node_path=get_bowtie2_path(),
                                                             bowtie2_index_path=get_bowtie2_index_path(),
                                                             bowtie2_index_name=get_bowtie2_index_name(),
                                                             bowtie2_number_threads=get_bowtie2_number_threads()))


        # ---------------------------------------- Start Streaming ----------------------------------------------------
        #
        #
        ssc.start()     # Start to schedule the Spark job on the underlying Spark Context.
        ssc.awaitTermination()      # Wait for the streaming computations to finish.
        ssc.stop()  # Stop the Streaming context



# -------------------------------------------- Stream from a Directory ------------------------------------------------
#
#
def dispatch_stream_from_kinesis(sampleID, sample_format, output_file, save_to_local, save_to_s3, partition_size, ssc,
                                 app_name, stream_name, endpoint_url, region_name, keep_shard_profiles,
                                 sensitive_align, annotations_dictionary, s3_output_bucket, coalesce_output,
                                 number_of_shards, sample_type, verbose_output, debug_mode, streaming_timeout):
    """
    Executes the requested Spark job in the cluster using a streaming strategy.
    Args:
        sampleID:               The unique id of the sample.
        sample_format:          What type of input format are the reads in (tab5, fastq, tab6, etc.).
        sample_type:            Are the reads single-end or paired-end.
        number_of_shards:       The number of shards that we'll be picking up from the Kinesis stream.
        output_file:            The path to the output file.
        save_to_s3:             Flag for storing output to AWS S3.
        save_to_local:          Flag for storing output to the local filesystem.
        partition_size:         Level of parallelization for RDDs that are not partitioned by the system.
        ssc:                    Spark Streaming Context.
        app_name:               Kinesis app name.
        stream_name:            Kinesis stream name.
        endpoint_url:           Kinesis Stream URL.
        region_name:            Amazon region name for the Kinesis stream.
        sensitive_align:        Sensitive Alignment Mode.
        annotations_dict:       Dictionary of Annotations for reporting organism names.
        s3_output_bucket:       The S3 bucket to write files into.
        keep_shard_profiles:    Retains the rolling shard profiles in S3 or the local filesystem.
        coalesce_output:        Merge output into a single file.
        verbose_output:         Flag for wordy terminal print statements.
        debug_mode:             Flag for debug mode. Activates slow checkpoints.
        streaming_timeout:      Time (in sec) after which streaming will stop.

    Returns:
        Nothing, if all goes well it should return cleanly.

    """

    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] ")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
          "] Stream Source: [KINESIS]")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Sample ID: " + sampleID +
          " (" + sample_format + ", " + sample_type + ")")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Stream Name: " + endpoint_url)
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Region: " + region_name)
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Streaming starting...")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] ")

    #   Set the number of shards so that we can safely exit after we have analyzed the requested number of shards.
    set_number_of_shards(int(number_of_shards))

    kinesis_decode = True

    #   Tab5-formatted FASTQ reads.
    if sample_format == "tab5":

        #
        #   Kinesis streaming
        #
        sample_dstream = KinesisUtils.createStream(ssc,
                                                   app_name,
                                                   stream_name,
                                                   endpoint_url,
                                                   region_name,
                                                   InitialPositionInStream.TRIM_HORIZON,
                                                   5)

        #
        #   Before we do anything, we have to move the data back to the Master. The way Spark Streaming works, is that
        #   the RDDs will be processed in the Worker in which they were received, which does not parallelize well.
        #   If we did not ship the input reads back to the master, then they would only be aligned in one Executor.
        #
        sc = ssc.sparkContext
        sample_dstream.foreachRDD(lambda rdd: profile_sample(sampleReadsRDD=rdd,
                                                             sc=sc,
                                                             ssc=ssc,
                                                             output_file=output_file,
                                                             save_to_s3=save_to_s3,
                                                             save_to_local=save_to_local,
                                                             sample_type=sample_type,
                                                             sensitive_align=sensitive_align,
                                                             annotations_dictionary=annotations_dictionary,
                                                             partition_size=partition_size,
                                                             s3_output_bucket=s3_output_bucket,
                                                             kinesis_decode=kinesis_decode,
                                                             keep_shard_profiles=keep_shard_profiles,
                                                             coalesce_output=coalesce_output,
                                                             verbose_output=verbose_output,
                                                             debug_mode=debug_mode,
                                                             streaming_timeout=streaming_timeout,
                                                             bowtie2_node_path=get_bowtie2_path(),
                                                             bowtie2_index_path=get_bowtie2_index_path(),
                                                             bowtie2_index_name=get_bowtie2_index_name(),
                                                             bowtie2_number_threads=get_bowtie2_number_threads()))


        # ---------------------------------------- Start Streaming ----------------------------------------------------
        #
        #
        ssc.start()     # Start to schedule the Spark job on the underlying Spark Context.
        ssc.awaitTermination()      # Wait for the streaming computations to finish.
        ssc.stop()  # Stop the Streaming context




# --------------------------------------------- Processing Functions --------------------------------------------------
#
#   This is where all the action is. This function gets called by both of the streaming job functions, and the code
#   for passing the data to Bowtie2, and receiving it back, is in these functions.
#
def profile_sample(sampleReadsRDD, sc, ssc, output_file, save_to_s3, save_to_local, sensitive_align, partition_size,
                   annotations_dictionary, s3_output_bucket, keep_shard_profiles, coalesce_output, verbose_output,
                   bowtie2_node_path, bowtie2_index_path, bowtie2_index_name, bowtie2_number_threads, sample_type,
                   debug_mode, streaming_timeout, kinesis_decode=None):

    #
    #   Nested inner function that gets called from the 'mapPartitions()' Spark function.
    #
    def align_with_bowtie2(iterator):
        """
        Function that runs on ALL worker nodes (Executors). Dispatches a Bowtie2 command and handles read alignments.
        Args:
            iterator:   Iterator object from Spark

        Returns:    An iterable collection of read alignments in SAM format.

        """
        alignments = []

        worker_node_ip = str(sp.check_output(["hostname"])).strip()

        #
        #   We pick up the RDD with reads that we set as a broadcast variable "previously" — The location of this action
        #   happens in the code below, which executes before 'this' code block.
        #
        reads_list = broadcast_sample_reads.value

        #   Obtain a properly formatted Bowtie2 command.
        bowtieCMD = getBowtie2Command(bowtie2_node_path=bowtie2_node_path,
                                      bowtie2_index_path=bowtie2_index_path,
                                      bowtie2_index_name=bowtie2_index_name,
                                      bowtie2_number_threads=bowtie2_number_threads)
        if sensitive_align:
            bowtieCMD = getBowtie2CommandSensitive(bowtie2_node_path=bowtie2_node_path,
                                                   bowtie2_index_path=bowtie2_index_path,
                                                   bowtie2_index_name=bowtie2_index_name,
                                                   bowtie2_number_threads=bowtie2_number_threads)


        #   Open a pipe to the subprocess that will launch the Bowtie2 aligner.
        try:
            align_subprocess = sp.Popen(bowtieCMD, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)

            pickled_reads_list = pickle.dumps(reads_list)
            # no_reads = len(reads_list)

            alignment_output, alignment_error = align_subprocess.communicate(input=pickled_reads_list.decode('latin-1'))

            #   The output is returned as a 'bytes' object, so we'll convert it to a list. That way, 'this' worker node
            #   will return a list of the alignments it found.
            for a_read in alignment_output.strip().decode().splitlines():

                #   Each alignment (in SAM format) is parsed and broken down into two (2) pieces: the read name,
                #   and the genome reference the read aligns to. We do the parsing here so that it occurs in the
                #   worker node and not in the master node. A benefit of parsing alignments in the worker node is
                #   that it also brings down the size of the 'alignment' object that gets transmitted through the
                #   network. Note that networking costs are minimal for a 'few' alignments, but they do add up
                #   for large samples with many shards.
                #
                #   SAM format: [0] - QNAME (the read name)
                #               [1] - FLAG
                #               [2] - RNAME (the genome reference name that the read aligns to
                #
                alignment = a_read.split("\t")[0] + "\t" + a_read.split("\t")[2]

                #   Once the alignment has been parsed, we add it to the return list of alignments that will be
                #   sent back to the master node.
                alignments.append(alignment)


        except sp.CalledProcessError as err:
            print( "[Flint - ALIGN ERROR] " + str(err))
            sys.exit(-1)

        return iter(alignments)


    # -----------------------------------------------------------------------------------------------------------------
    #
    #
    def get_organism_name(gca_id):
        """
        Nested function for retrieving and constructing a proper organism name for the reports.
        Args:
            gca_id: The 'GCA' formatted ID to look up in the annotations.

        Returns:
            A properly formatted string for the organism name that contains a Taxa_ID, Genus-Species-Strain name,
            and the GCA_ID that was used for the query.
        """

        organism_name_string = ""

        if gca_id in annotations_dictionary:
            taxa_id = annotations_dictionary[gca_id]['taxa_id']
            organism_name = annotations_dictionary[gca_id]['organism_name']

            organism_name_string = str(taxa_id) + "\t" + str(gca_id) + "\t" + str(organism_name)

        else:
            organism_name_string = gca_id

        return organism_name_string


    # -----------------------------------------------------------------------------------------------------------------
    #
    #
    def accumulate_abundaces(a_strain):
        print("Hello from accumulate_abundances()")
        strain_name = a_strain[0]
        abundance_count = a_strain[1]

        # abundances = get_overall_abundaces()
        global OVERALL_ABUNDANCES
        OVERALL_ABUNDANCES += {strain_name: abundance_count}

        return a_strain


    # -----------------------------------------------------------------------------------------------------------------
    #
    #   The main 'profileSample()' function starts here.
    #
    try:
        if not sampleReadsRDD.isEmpty():

            if get_shard_counter() == 0:
                set_analysis_start_time()

            # -------------------------------------- Alignment --------------------------------------------------------
            #
            #   First, we'll convert the RDD to a list, which we'll then convert to a Spark.broadcast variable
            #   that will be shipped to all the Worker Nodes (and their Executors) for read alignment.
            #
            if kinesis_decode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Decoding Kinesis Stream...")
                sample_reads_list = json.loads(sampleReadsRDD.collect())
            else:
                sample_reads_list = sampleReadsRDD.collect()    # collect returns <type 'list'> on the main driver.

            number_input_reads = len(sample_reads_list)

            #
            #   The RDD with reads is set as a Broadcast variable that will be picked up by each worker node.
            #
            broadcast_sample_reads = sc.broadcast(sample_reads_list)


            #
            #   Run starts here.
            #
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Shard: " +
                  str(get_shard_counter()) + " of " + str(get_shards()))

            read_noun = ""
            if sample_type.lower() == "paired":
                read_noun == "Paired-End"

            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Input: " +
                  '{:0,.0f}'.format(number_input_reads) + " " + read_noun + " Reads.")

            alignment_start_time = time.time()

            data = sc.parallelize(range(1, partition_size))
            data_num_partitions = data.getNumPartitions()

            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] No. RDD Partitions: " +
                      str(data_num_partitions))

            #
            #   Dispatch the Alignment job with Bowtie2
            #
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] Aligning reads with Bowtie2 (" + str(bowtie2_number_threads) + ")...")

            if sensitive_align:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Using Sensitive Alignment Mode...")

            alignments_RDD = data.mapPartitions(align_with_bowtie2)
            number_of_alignments = alignments_RDD.count()

            alignment_end_time = time.time()
            alignment_total_time = alignment_end_time - alignment_start_time

            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Bowtie2 - Complete. " +
                  "(" + str(timedelta(seconds=alignment_total_time)) + ")")
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]" + " Found: " +
                  '{:0,.0f}'.format(number_of_alignments) + " Alignments.")

            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] Analyzing...")

            # ------------------------------------------- Map 1 -------------------------------------------------------
            #
            #   The Map step sets up the basic data structure that we start with — a map of reads to the genomes they
            #   align to. We'll Map a read (QNAME) with a genome reference name (RNAME).
            #   The REDUCE 1 step afterwards will collect all the genomes and key them to a unique read name.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Mapping Reads to Genomes (QNAME-RNAME).")

            map_reads_to_genomes = alignments_RDD.map(lambda line: (line.split("\t")[0], [line.split("\t")[1]])).cache()

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 1: Map 1, map_reads_to_genomes")
                chk_1_s = time.time()
                checkpoint_1 = map_reads_to_genomes.count()
                chk_1_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_1_e - chk_1_s))))


            if verbose_output:
                #   We use the following block to get the Overall Mapping Rate for 'this' shard.
                list_unique_reads = map_reads_to_genomes.flatMap(lambda x: x).keys().distinct()
                number_of_reads_aligned = list_unique_reads.count()

                overall_mapping_rate = float(number_of_reads_aligned) / number_input_reads
                overall_mapping_rate = overall_mapping_rate * 100

                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Shard Mapping Rate: " +
                      '{:.2f}'.format(overall_mapping_rate) + "%")


            # ------------------------------------------ Reduce 1 -----------------------------------------------------
            #
            #   Reduce will operate first by calculating the read contributions, and then using these contributions
            #   to calculate an abundance.
            #   Note the "+" operator can be used to concatenate two lists. :)
            #

            #   'Reduce by Reads' will give us a 'dictionary-like' data structure that contains a Read Name (QNAME) as
            #   the KEY, and a list of genome references (RNAME) as the VALUE. This allows us to calculate
            #   each read's contribution.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Reducing Reads to list of Genomes...")

            reads_to_genomes_list = map_reads_to_genomes.reduceByKey(lambda l1, l2: l1 + l2)

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 2: Reduce 1, reads_to_genomes_list")
                chk_2_s = time.time()
                checkpoint_2 = reads_to_genomes_list.count()
                chk_2_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_2_e - chk_2_s))))


            # ------------------------------------ Map 2, Fractional Reads --------------------------------------------
            #
            #   Read Contributions.
            #   Each read is normalized by the number of genomes it maps to. The idea is that reads that align to
            #   multiple genomes will contribute less (have a hig denominator) than reads that align to fewer genomes.
            #

            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Calculating Read Contributions...")

            read_contributions = reads_to_genomes_list.mapValues(lambda l1: 1 / float(len(l1)))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 3: Map 2, read_contributions")
                chk_3_s = time.time()
                checkpoint_3 = read_contributions.count()
                chk_3_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_3_e - chk_3_s))))


            #
            #   Once we have the read contributions, we'll JOIN them with the starting 'map_reads_to_genomes' RDD to
            #   get an RDD that will map the read contribution to the Genome it aligns to.
            #   Note: l[0] is the Read name (key), and l[1] is the VALUE (a list) after the join.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Joining Read Contributions to Genomes...")

            read_contribution_to_genome = read_contributions.join(map_reads_to_genomes)\
                                                            .map(lambda l: (l[1][0], "".join(l[1][1])))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 4: read_contribution_to_genome")
                chk_4_s = time.time()
                checkpoint_4 = read_contribution_to_genome.count()
                chk_4_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_4_e - chk_4_s))))


            #
            #   Now have an RDD mapping the Read Contributions to Genome Names, we'll flip the (KEY,VALUE) pairings
            #   so that we have Genome Name as KEY and Read Contribution as Value.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Flipping Genomes and Read Contributions...")

            genomes_to_read_contributions = read_contribution_to_genome.map(lambda x: (x[1], x[0]))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 5: genomes_to_read_contributions")
                chk_5_s = time.time()
                checkpoint_5 = genomes_to_read_contributions.count()
                chk_5_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_5_e - chk_5_s))))


            # --------------------------------------- Reduce 2, Abundances --------------------------------------------
            #
            #   Following the KEY-VALUE inversion, we do a reduceByKey() to aggregate the fractional counts for a
            #   given genomic assembly and calculate its abundance.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Calculating Genome Abundances...")

            genomic_assembly_abundances = genomes_to_read_contributions.reduceByKey(lambda l1, l2: l1 + l2)

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 6: Reduce 2, genomic_assembly_abundances")
                chk_6_s = time.time()
                checkpoint_6 = genomic_assembly_abundances.count()
                chk_6_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_6_e - chk_6_s))))


            #
            #   Strain level abundances
            #   At this point we have abundances at the genomic-assembly level (chromosomes, contigs, etc.), but what
            #   we are after is abundances one level higher, i.e., at the Strain level. So we'll do one more map to
            #   to set a key that we can reduce with at the Strain level.
            #
            #   Note: The key to map here is critical. The assembly FASTA files need to be in a format that tells us
            #   how to 'fold up' the assemblies into a parent taxa. The FASTA files we indexed had a Taxonomic ID
            #   that tells us the Organism name (at the Strain level), and it is delimited by a period and located
            #   at the beginning of the FASTA record.
            #
            strain_map = genomic_assembly_abundances.map(lambda x: ("GCA_" + x[0].split(".")[0], x[1]))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 7: strain_map")
                chk_7_s = time.time()
                checkpoint_7 = strain_map.count()
                chk_7_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_7_e - chk_7_s))))


            #
            #   Once the Mapping of organism names at the Strain level is complete, we can just Reduce them to
            #   aggregate the Strain-level abundances.
            #
            strain_abundances = strain_map.reduceByKey(lambda l1, l2: l1 + l2).cache()

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 8: strain_abundances")
                chk_8_s = time.time()
                checkpoint_8 = strain_abundances.count()
                chk_8_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_8_e - chk_8_s))))


            # --------------------------------------- Abundance Coalescing --------------------------------------------
            #
            #   If requested, we'll continously update the rolling count of abundances for the strains that we've
            #   seen, or add new ones. Note that this involves a call to 'collect()' which brings back everything
            #   to the Master node, so there is a slight hit on performance.
            #
            if coalesce_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Updating abundance counts...")

                #   Use 'collect()' to aggregate the counts for 'this' Shard and accumulate the rolling count.
                #   We are violating the prime directive of Spark design patterns by using 'collect()', but in
                #   practice, the call adds a negligible amount to the running time.
                # list_strain_abundances = strain_abundances.collect()
                #
                # for a_strain in list_strain_abundances:
                #     strain_name     = a_strain[0]
                #     abundance_count = a_strain[1]
                #
                #     if strain_name in OVERALL_ABUNDANCES:
                #         OVERALL_ABUNDANCES[strain_name] += abundance_count
                #     else:
                #         OVERALL_ABUNDANCES[strain_name] = abundance_count

                abundace_rdd    = strain_abundances.map(lambda x: accumulate_abundaces(x)).cache()
                abundance_count = abundace_rdd.count()

            else:
                if save_to_s3:
                    output_file = output_file.replace("/abundances.txt", "")
                    output_dir_s3_path = "s3a://" + s3_output_bucket + "/" + output_file + "/shard_" + \
                                         str(RDD_COUNTER) + "/"

                    strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                        .saveAsTextFile(output_dir_s3_path)

                #   Careful with this. This will cause the individual files to be stored in Worker nodes, and
                #   not in the Master node.
                #   TODO: Refactor so that it sends it back to the Master, and stores it in the 'local' master path.
                # if save_to_local:
                #     output_dir_local_path = output_file.replace("abundances.txt", "/shard_" + str(RDD_COUNTER))
                #     abundances_list = strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                #         .saveAsTextFile("file://" + output_dir_local_path)


            # ------------------------------------------ Shard Profiles -----------------------------------------------
            #
            #   The user can specify whether to retain individual Shard profiles. The flag that controls this
            #   'keep_shard_profiles' works in conjuction with the '' and '' flags. So the rolling shard prolies
            #   will be stored in the location that the user requested.
            #
            if keep_shard_profiles:
                # -------------------------------------- S3 Rolling Output --------------------------------------------
                if save_to_s3:
                    if verbose_output:
                        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                              "] Saving to S3 bucket...")

                    #   Pointer to S3 filesystem.
                    sc._jsc.hadoopConfiguration().set("mapred.output.committer.class",
                                                      "org.apache.hadoop.mapred.FileOutputCommitter")
                    URI             = sc._gateway.jvm.java.net.URI
                    Path            = sc._gateway.jvm.org.apache.hadoop.fs.Path
                    FileSystem      = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
                    fs_uri_string   = "s3a://" + s3_output_bucket + ""
                    fs              = FileSystem.get(URI(fs_uri_string), sc._jsc.hadoopConfiguration())

                    shard_sub_dir = "shard-profiles"
                    output_file = output_file.replace("/abundances.txt", "")
                    shard_sub_dir_path = "s3a://" + s3_output_bucket + "/" + output_file + "/" + shard_sub_dir

                    #   Create the 1st temporary output file through the 'saveAsTextFile()' method.
                    tmp_output_file = shard_sub_dir_path + "-tmp"

                    strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                                     .coalesce(1) \
                                     .saveAsTextFile(tmp_output_file)

                    #   This is the "tmp" file created by "saveAsTextFile()", by default its named "part-00000").
                    created_file_path = Path(tmp_output_file + "/part-00000")

                    #   The gimmick here is to move the tmp file into a final location so that "saveAsTextFile()"
                    #   can write again.
                    str_for_rename = shard_sub_dir_path + "/abundances-" + str(RDD_COUNTER) + ".txt"
                    renamed_file_path = Path(str_for_rename)

                    #   Create the directory in which we'll be storing the shard profiles.
                    fs.mkdirs(Path(shard_sub_dir_path))

                    #   The "rename()" function can be used as a "move()" if the second path is different.
                    fs.rename(created_file_path, renamed_file_path)

                    #   Remove the "tmp" directory we created when we used "saveAsTextFile()".
                    fs.delete(Path(tmp_output_file))


                # ------------------------------------ Local Rolling Output -------------------------------------------
                if save_to_local:
                    if verbose_output:
                        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                              "] Saving to local filesystem...")

                    rdd_counter_str = "-" + str(RDD_COUNTER) + ".txt"
                    output_file = output_file.replace("abundances", "/shard_profiles/abundances")
                    output_file = output_file.replace(".txt", rdd_counter_str)

                    writer = csv.writer(open(output_file, "wb"), delimiter='|', lineterminator="\n", quotechar='',
                                        quoting=csv.QUOTE_NONE)

                    abundances_list = strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                                                    .repartition(1).collect()
                    for a_line in abundances_list:
                        writer.writerow([a_line])


            # -------------------------------------------- End of Run -------------------------------------------------
            #
            #   Housekeeping tasks go here. This completes the processing of a single streamed shard.

            #   Increment the counter that we use to keep track of, and also use as an affix for a RDDs profile count.
            increment_rdd_count()

            #   Set the time at which 'this' RDD (a sample shard) was last processed.
            set_time_of_last_rdd(time.time())

            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Done.")
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]")


        # ---------------------------------------------- Empty RDD Case -----------------------------------------------
        #
        else:
            #   If we've analyzed the same number of shards as those that were requested, we stop the streaming.
            #   How we stop streaming, it depends on what flags were set. We'll stop the streaming if the time
            #   between the last RDD processed and now is greater than a user-defined timeout, or 3 seconds for
            #   the default. Another way to stop is to have processed a certain number of shards. If this number
            #   has been reached, then we'll go ahead and stop.

            time_of_last_check = get_time_of_last_rdd()
            time_now = time.time()
            check_delta = timedelta(seconds=(time_now - time_of_last_check))
            check_delta_int = int(check_delta.seconds)

            if time_of_last_check != 0:
                if shard_equals_counter() or check_delta_int > streaming_timeout:
                    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                          "] All Requested Sample Shards Finished. (" + str(get_shards()) + " shards)")

                    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Stopping Streaming.")

                    set_analysis_end_time()

                    #   The last thing we do is to stop the streaming context. Once this command finishes, we are
                    #   jumped back-out into the code-block that called us — the 'flint.py' script.
                    ssc.stop()

    except Exception as ex:
        template = "[Flint - ERROR] An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)





# --------------------------------------------- Non-Streaming Job -----------------------------------------------------
#
#
def dispatch_local_job(mate_1, mate_2, tab5File, sampleID, sample_format, output_file,
                       save_to_s3, partition_size, sc):
    """
    Executes the requested Spark job in the cluster in a non-streaming method.
    Args:
        mate_1: Paired-end reads left-side read.
        mate_2: Paired-end reads right-side read.
        tab5File:   Sample reads file in tab5 format.
        sampleID:   The unique id of the sample.
        sample_format: What type of input format are the reads in (tab5, fastq, tab6, etc.).
        output_file:    The path to the output file.
        save_to_s3: Flag for storing output to AWS S3.
        partition_size: Level of parallelization for RDDs that are not partitioned by the system.
        sc:     Spark Context.

    Returns:
        Nothing, if all goes well it should return cleanly.
    """

    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] ")
    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]" + " Analyzing Sample " + sampleID +
          " (" + sample_format + ")")

    #
    #   Paired-end Reads Code path.
    #
    if sample_format == "tab5":

        # ------------------------------------------ Alignment ----------------------------------------------------
        #
        #   Alignment of sample reads to the index in each of the workers is delegated to Bowtie2, and getting the
        #   reads to bowtie2 is performed through Spark's most-excellent pipe() function that sends the contents
        #   of the RDD to the STDIN of the mapping script. The mapping script communicates with bowtie2 and outputs
        #   the alignments to STDOUT which is captured by Spark and returned to us as an RDD.
        #

        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Loading Sample...")
        sampleRDD = loadTab5File(sc, tab5File)
        sampleReadsRDD = sampleRDD.values()

        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Starting Alignment with Bowtie2...")
        alignment_start_time = time.time()

        alignmentsRDD = sampleReadsRDD.pipe(dnaMappingScript)

        numberOfAlignments = alignmentsRDD.count()

        alignment_end_time = time.time()
        alignment_total_time = alignment_end_time - alignment_start_time

        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Bowtie2 - Complete. " +
              "(" + str(timedelta(seconds=alignment_total_time)) + ")")
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]" + " Found: " +
              '{:0,.0f}'.format(numberOfAlignments) + " Alignments.")
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]")


        # --------------------------------------------- Map -------------------------------------------------------
        #
        #   The Map step sets up the basic data structure that we start with — a map of reads to the genomes they
        #   align to.  Each alignment (in SAM format) is parsed an broken down into two (2) pieces: the read name,
        #   and the genome reference.  The REDUCE step afterwards will collect all the genomes and key them to a
        #   unique read name.
        #
        #   SAM format: [0] - QNAME (the read name)
        #               [1] - FLAG
        #               [2] - RNAME (the genome reference name that the read aligns to

        #   We'll Map a read (QNAME) with a genome reference name (RNAME).
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] MAP - Reads to Genomes (QNAME-RNAME).")
        mapReadToGenomes    = alignmentsRDD.map(lambda line: (line.split("\t")[0], [line.split("\t")[2]] ))


        # -------------------------------------------- Reduce -----------------------------------------------------
        #
        #   Reduce will operate first by calculating the read contributions, and then using these contributions
        #   to calculate an abundance.
        #   Note the "+" operator can be used to concatenate two lists. :)
        #

        #   'Reduce by Reads' will give us a 'dictionary-like' data structure that contains a Read Name (QNAME) as
        #   the KEY, and a list of genome references (RNAME) as the VALUE. This allows us to calculate
        #   each read's contribution.
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] REDUCE - Reads to list of Genomes.")
        readsToGenomesList = mapReadToGenomes.reduceByKey(lambda l1, l2: l1 + l2)


        # ---------------------------------- Fractional Reads & Abundances ----------------------------------------
        #
        #   Read Contributions.
        #   Each read is normalized by the number of genomes it maps to. The idea is that reads that align to
        #   multiple genomes will contribute less (have a hig denominator) than reads that align to fewer genomes.

        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]")
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Calculating Read Contributions...")
        readContributions = readsToGenomesList.mapValues(lambda l1: 1/float(len(l1)))

        #   Once we have the read contributions, we'll JOIN them with the starting mapReadToGenomes RDD to get an
        #   RDD that will map the read contribution to the Genome it aligns to.
        #   Note: l[0] is the Read name (key), and l[1] is the VALUE (a list) after the join.
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
              "] Joining Read Contributions to Genomes...")

        readContributionToGenome = readContributions.join(mapReadToGenomes)\
                                                    .map(lambda l: (l[1][0], "".join(l[1][1])))

        #   After we have the RDD mapping the Read Contributions to Genome Names, we'll flip the (KEY,VALUE) pairings
        #   so that we have Genome Name as KEY and Read Contribution as Value.
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
              "] Flipping Genomes and Read Contributions...")

        genomesToReadContributions = readContributionToGenome.map(lambda x: (x[1], x[0]))

        #   Following the KEY-VALUE inversion, we do a reduceByKey() to aggregate the fractional counts for a
        #   given Genome and calculate its abundance.
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Calculating Genome Abundances...")

        genomeAbundances = genomesToReadContributions.reduceByKey(lambda l1, l2: l1 + l2)


        # ------------------------------------------ Output Reports -----------------------------------------------
        #
        #   Reports are written out to an S3 bucket specified in the initial JSON config file.
        #
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]")
        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Writing Output Reports...")

        if save_to_s3:

            #   We map the abundances so that we get a nice tab-delimited file, then repartition it so that we only
            #   get a single file, and not multiple ones for each partition.
            genomeAbundances.map(lambda x: "%s\t%s" %(x[0],x[1]))\
                            .repartition(1)\
                            .saveAsTextFile(output_file)

        else:
            writer = csv.writer(open(output_file, "wb"), delimiter='\t', lineterminator="\n")
            abundances = genomeAbundances.collect()
            writer.writerow(abundances)


        return



# ----------------------------------------------- Helper Functions ----------------------------------------------------
#
#   Miscellaneous helper functions for Mapping, accumulating, reducing, etc.
#

def loadTab5File(sc, pathToSampleFile):
    """
    Loads a Tab5-formatted file into an RDD. The file is loaded using the Hadoop File API so we can create an RDD
    based on the tab5 format: [name]\t[seq1]\t[qual1]\t[seq2]\t[qual2]\n
    Args:
        sc: A Spark context
        pathToSampleFile: A valid Hadoop file path (S3, HDFS, etc.).

    Returns:
        An RDD with a record number as KEY, and reads as VALUE.
    """
    sampleRDD = sc.newAPIHadoopFile(pathToSampleFile,
                                    'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                                    'org.apache.hadoop.io.LongWritable',
                                    'org.apache.hadoop.io.Text',
                                    conf={'textinputformat.record.delimiter': '\n'})

    return sampleRDD


def loadFASTQFile(sc, pathToSampleFile):
    """
    Loads a FASTQ file into an RDD. The file is loaded using the Hadoop File API so we can create an RDD based on the
    FASTQ file format, i.e., multiline parsing of the file.

    Args:
        sc: A Spark context
        pathToSampleFile: A valid Hadoop file path (S3, HDFS, etc.).

    Returns:
        An RDD with a record number as KEY, and read attributes as VALUE.
    """

    sampleRDD = sc.newAPIHadoopFile(pathToSampleFile,
                                    'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                                    'org.apache.hadoop.io.LongWritable',
                                    'org.apache.hadoop.io.Text',
                                    conf={'textinputformat.record.delimiter': '\n@'})

    return sampleRDD


def getBowtie2Command(bowtie2_node_path, bowtie2_index_path, bowtie2_index_name, bowtie2_number_threads):
    """
    Constructs a properly formatted shell Bowtie2 command by performing a simple lexical analysis using 'shlex.split()'.
    Returns:
        An array with the bowtie 2 command call split into an array that can be used by the popen() function.
    """

    index_location  = bowtie2_index_path
    index_name      = bowtie2_index_name
    index = index_location + "/" + index_name
    number_of_threads = bowtie2_number_threads

    bowtieCMD = bowtie2_node_path + '/bowtie2 \
                                    --threads ' + str(number_of_threads) + ' \
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
                                    -x ' + index + ' --tab5 -'

    return shlex.split(bowtieCMD)


def getBowtie2CommandSensitive(bowtie2_node_path, bowtie2_index_path, bowtie2_index_name, bowtie2_number_threads):
    """
    Constructs a properly formatted shell Bowtie2 command by performing a simple lexical analysis using 'shlex.split()'.
    Returns:
        An array with the bowtie 2 command call split into an array that can be used by the popen() function.
    """

    index_location  = bowtie2_index_path
    index_name      = bowtie2_index_name
    index = index_location + "/" + index_name

    bowtieCMD = bowtie2_node_path + '/bowtie2 \
                                    --threads 6 \
                                    --local \
                                    -D 20 \
                                    -R 3 \
                                    -N 0 \
                                    -L 20 \
                                    -i \'"S,1,0.50"\' \
                                    --no-sq \
                                    --no-hd \
                                    --no-unal \
                                    -q \
                                    -x ' + index + ' --tab5 -'

    return shlex.split(bowtieCMD)
