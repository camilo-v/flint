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
import subprocess as sp
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream


# ----------------------------------------------- Streaming Job -------------------------------------------------------
#
#
def dispatchSparkStreamingJob(stream_source_dir, batch_duration, dnaMappingScript, sampleID, sampleType, output_file,
                              save_to_s3, partition_size, ssc, app_name, stream_name, endpoint_url, region_name,
                              annotations, output_directory, sensitive_align, annotations_dictionary):
    """
    Executes the requested Spark job in the cluster using a streaming strategy.
    Args:
        stream_source_dir:  The directory to stream files from.
        batch_duration:     Batch duration for streaming, in seconds.
        dnaMappingScript:   The delegate that handles the bowtie2 communications.
        sampleID:           The unique id of the sample.
        sampleType:         What type of input format are the reads in (tab5, fastq, tab6, etc.).
        output_file:        The path to the output file.
        save_to_s3:         Flag for storing output to AWS S3.
        partition_size:     Level of parallelization for RDDs that are not partitioned by the system.
        ssc:                Spark Streaming Context.
        app_name:           Kinesis app name.
        stream_name:        Kinesis stream name.
        endpoint_url:       Kinesis Stream URL.
        region_name:        Amazon region name for the Kinesis stream.
        annotations:        Annotations file for profile abundances.
        output_directory:   Output directory for abundance profiles.
        sensitive_align:    Sensitive Alignment Mode.
        annotations_dict:   Dictionary of Annotations for reporting organism names.

    Returns:
        Nothing, if all goes well it should return cleanly.

    """

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] *********** Stream Processing *********** ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Analyzing Sample: " + sampleID +
          " (" + sampleType + ")")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Stream: " + endpoint_url)
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Region: " + region_name)
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Batch Duration: " +
          str(batch_duration) + " sec")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Streaming starting...")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")

    #
    #   Paired-end Reads Code path.
    #
    if sampleType == "tab5":

        #
        #   Filesystem Streaming
        #
        sample_dstream = ssc.textFileStream(stream_source_dir)

        #
        #   Before we do anything, we have to move the data back to the Master. The way Spark Streaming works, is that
        #   the RDDs will be processed in the Worker in which they were received, which does not parallelize well.
        #   If we did not ship the input reads back to the master, then they would only be aligned in one Executor.
        #
        sc = ssc.sparkContext
        sample_dstream.foreachRDD(lambda rdd: profileSample(sampleReadsRDD=rdd,
                                                            dnaMappingScript=dnaMappingScript,
                                                            sc=sc,
                                                            annotations=annotations,
                                                            output_directory=output_directory,
                                                            output_file=output_file,
                                                            save_to_s3=save_to_s3,
                                                            sensitive_align=sensitive_align,
                                                            annotations_dictionary=annotations_dictionary,
                                                            partition_size=partition_size))


        # ---------------------------------------- Start Streaming ----------------------------------------------------
        #
        #
        ssc.start()     # Start to schedule the Spark job on the underlying Spark Context.
        ssc.awaitTermination()      # Wait for the streaming computations to finish.
        ssc.stop()  # Stop the Streaming context


# --------------------------------------------- Processing Functions --------------------------------------------------
#
#
def profileSample(sampleReadsRDD, dnaMappingScript, sc, annotations, output_directory, output_file, save_to_s3,
                  sensitive_align, annotations_dictionary, partition_size):

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
        bowtieCMD = getBowtie2Command()
        if sensitive_align:
            bowtieCMD = getBowtie2CommandSensitive()


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
    #   The main 'profileSample()' function starts here.
    #
    try:
        if not sampleReadsRDD.isEmpty():

            # -------------------------------------- Alignment --------------------------------------------------------
            #
            #   First, we'll convert the RDD to a list, which we'll then convert to a Spark.broadcast variable
            #   that will be shipped to all the Worker Nodes (and their Executors) for read alignment.
            #
            sample_reads_list = sampleReadsRDD.collect()    # collect returns <type 'list'> on the main driver.
            number_input_reads = len(sample_reads_list)

            #
            #   The RDD with reads is set as a Broadcast variable that will be picked up by each worker node.
            #
            broadcast_sample_reads = sc.broadcast(sample_reads_list)

            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Input: " +
                  '{:0,.0f}'.format(number_input_reads) + " Reads.")

            alignment_start_time = time.time()

            data = sc.parallelize(range(1, partition_size))
            data_num_partitions = data.getNumPartitions()

            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] No. RDD Partitions: " +
                  str(data_num_partitions))

            #
            #   Dispatch the Alignment job with Bowtie2
            #
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Aligning reads with Bowtie2...")

            if sensitive_align:
                print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      " ] Using Sensitive Alignment Mode...")

            alignments_RDD = data.mapPartitions(align_with_bowtie2)
            number_of_alignments = alignments_RDD.count()

            alignment_end_time = time.time()
            alignment_total_time = alignment_end_time - alignment_start_time

            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Bowtie2 - Complete. " +
                  "(" + str(timedelta(seconds=alignment_total_time)) + ")")
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" + " Found: " +
                  '{:0,.0f}'.format(number_of_alignments) + " Alignments.")


            # ------------------------------------------- Map 1 -------------------------------------------------------
            #
            #   The Map step sets up the basic data structure that we start with — a map of reads to the genomes they
            #   align to. We'll Map a read (QNAME) with a genome reference name (RNAME).
            #   The REDUCE 1 step afterwards will collect all the genomes and key them to a unique read name.
            #
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  " ] Mapping Reads to Genomes (QNAME-RNAME).")

            map_reads_to_genomes = alignments_RDD.map(lambda line: (line.split("\t")[0], [line.split("\t")[1]]))

            #   Get the unique reads that were mapped.
            list_unique_reads = map_reads_to_genomes.flatMap(lambda x: x).keys().distinct().collect()
            number_of_reads_aligned = len(list_unique_reads)

            overall_mapping_rate = float(number_of_reads_aligned) / number_input_reads
            overall_mapping_rate = overall_mapping_rate * 100

            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Overall Mapping Rate: " +
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
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  " ] Reducing Reads to list of Genomes...")

            reads_to_genomes_list = map_reads_to_genomes.reduceByKey(lambda l1, l2: l1 + l2)


            # ------------------------------------ Map 2, Fractional Reads --------------------------------------------
            #
            #   Read Contributions.
            #   Each read is normalized by the number of genomes it maps to. The idea is that reads that align to
            #   multiple genomes will contribute less (have a hig denominator) than reads that align to fewer genomes.
            #

            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Calculating Read Contributions...")

            read_contributions = reads_to_genomes_list.mapValues(lambda l1: 1 / float(len(l1)))

            #
            #   Once we have the read contributions, we'll JOIN them with the starting 'map_reads_to_genomes' RDD to
            #   get an RDD that will map the read contribution to the Genome it aligns to.
            #   Note: l[0] is the Read name (key), and l[1] is the VALUE (a list) after the join.
            #
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  " ] Joining Read Contributions to Genomes...")

            read_contribution_to_genome = read_contributions.join(map_reads_to_genomes)\
                                                            .map(lambda l: (l[1][0], "".join(l[1][1])))

            #
            #   Now have an RDD mapping the Read Contributions to Genome Names, we'll flip the (KEY,VALUE) pairings
            #   so that we have Genome Name as KEY and Read Contribution as Value.
            #
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  " ] Flipping Genomes and Read Contributions...")

            genomes_to_read_contributions = read_contribution_to_genome.map(lambda x: (x[1], x[0]))


            # --------------------------------------- Reduce 2, Abundances --------------------------------------------
            #
            #   Following the KEY-VALUE inversion, we do a reduceByKey() to aggregate the fractional counts for a
            #   given genomic assembly and calculate its abundance.
            #
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Calculating Genome Abundances...")

            genomic_assembly_abundances = genomes_to_read_contributions.reduceByKey(lambda l1, l2: l1 + l2)

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

            #
            #   Once the Mapping of organism names at the Strain level is complete, we can just Reduce them to
            #   aggregate the Strain-level abundances.
            #
            strain_abundances = strain_map.reduceByKey(lambda l1, l2: l1 + l2)


            # ------------------------------------------ Output Reports -----------------------------------------------
            #
            #   Reports are written out to an S3 bucket specified in the initial JSON config file.
            #
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Writting Output Reports...")

            if save_to_s3:
                #
                #   We map the abundances so that we get a nice tab-delimited file, then repartition it so that we only
                #   get a single file, and not multiple ones for each partition.
                #
                strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                                 .repartition(1) \
                                 .saveAsTextFile(output_file)


            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Done.")
            print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")


    except Exception as ex:
        template = "[Flint - ERROR] An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print message





# --------------------------------------------- Non-Streaming Job -----------------------------------------------------
#
#
def dispatchSparkJob(mate_1, mate_2, tab5File, dnaMappingScript, sampleID, sampleType, output_file,
                     save_to_s3, partition_size, sc):
    """
    Executes the requested Spark job in the cluster in a non-streaming method.
    Args:
        mate_1: Paired-end reads left-side read.
        mate_2: Paired-end reads right-side read.
        tab5File:   Sample reads file in tab5 format.
        dnaMappingScript:   The delegate that handles the bowtie2 communications.
        sampleID:   The unique id of the sample.
        sampleType: What type of input format are the reads in (tab5, fastq, tab6, etc.).
        output_file:    The path to the output file.
        save_to_s3: Flag for storing output to AWS S3.
        partition_size: Level of parallelization for RDDs that are not partitioned by the system.
        sc:     Spark Context.

    Returns:
        Nothing, if all goes well it should return cleanly.
    """

    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] ")
    print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" + " Analyzing Sample " + sampleID +
          " (" + sampleType + ")")

    #
    #   Paired-end Reads Code path.
    #
    if sampleType == "tab5":

        # ------------------------------------------ Alignment ----------------------------------------------------
        #
        #   Alignment of sample reads to the index in each of the workers is delegated to Bowtie2, and getting the
        #   reads to bowtie2 is performed through Spark's most-excellent pipe() function that sends the contents
        #   of the RDD to the STDIN of the mapping script. The mapping script communicates with bowtie2 and outputs
        #   the alignments to STDOUT which is captured by Spark and returned to us as an RDD.
        #

        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Loading Sample...")
        sampleRDD = loadTab5File(sc, tab5File)
        sampleReadsRDD = sampleRDD.values()

        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Starting Alignment with Bowtie2...")
        alignment_start_time = time.time()

        alignmentsRDD = sampleReadsRDD.pipe(dnaMappingScript)

        numberOfAlignments = alignmentsRDD.count()

        alignment_end_time = time.time()
        alignment_total_time = alignment_end_time - alignment_start_time

        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Bowtie2 - Complete. " +
              "(" + str(timedelta(seconds=alignment_total_time)) + ")")
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]" + " Found: " +
              '{:0,.0f}'.format(numberOfAlignments) + " Alignments.")
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")


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
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] MAP - Reads to Genomes (QNAME-RNAME).")
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
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] REDUCE - Reads to list of Genomes.")
        readsToGenomesList = mapReadToGenomes.reduceByKey(lambda l1, l2: l1 + l2)


        # ---------------------------------- Fractional Reads & Abundances ----------------------------------------
        #
        #   Read Contributions.
        #   Each read is normalized by the number of genomes it maps to. The idea is that reads that align to
        #   multiple genomes will contribute less (have a hig denominator) than reads that align to fewer genomes.

        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Calculating Read Contributions...")
        readContributions = readsToGenomesList.mapValues(lambda l1: 1/float(len(l1)))

        #   Once we have the read contributions, we'll JOIN them with the starting mapReadToGenomes RDD to get an
        #   RDD that will map the read contribution to the Genome it aligns to.
        #   Note: l[0] is the Read name (key), and l[1] is the VALUE (a list) after the join.
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
              " ] Joining Read Contributions to Genomes...")

        readContributionToGenome = readContributions.join(mapReadToGenomes)\
                                                    .map(lambda l: (l[1][0], "".join(l[1][1])))

        #   After we have the RDD mapping the Read Contributions to Genome Names, we'll flip the (KEY,VALUE) pairings
        #   so that we have Genome Name as KEY and Read Contribution as Value.
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
              " ] Flipping Genomes and Read Contributions...")

        genomesToReadContributions = readContributionToGenome.map(lambda x: (x[1], x[0]))

        #   Following the KEY-VALUE inversion, we do a reduceByKey() to aggregate the fractional counts for a
        #   given Genome and calculate its abundance.
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Calculating Genome Abundances...")
        genomeAbundances = genomesToReadContributions.reduceByKey(lambda l1, l2: l1 + l2)


        # ------------------------------------------ Output Reports -----------------------------------------------
        #
        #   Reports are written out to an S3 bucket specified in the initial JSON config file.
        #
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ]")
        print("[ " + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + " ] Writting Output Reports...")

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


def getBowtie2Command():
    """
    Constructs a properly formatted shell Bowtie2 command by performing a simple lexical analysis using 'shlex.split()'.
    Returns:
        An array with the bowtie 2 command call split into an array that can be used by the popen() function.
    """

    index_location = '/mnt/bio_data/index'
    index_name = 'ensembl_v41'

    index = index_location + "/" + index_name

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
                                    -x ' + index + ' --tab5 -'

    return shlex.split(bowtieCMD)


def getBowtie2CommandSensitive():
    """
    Constructs a properly formatted shell Bowtie2 command by performing a simple lexical analysis using 'shlex.split()'.
    Returns:
        An array with the bowtie 2 command call split into an array that can be used by the popen() function.
    """

    index_location = '/mnt/bio_data/index'
    index_name = 'ensembl_v41'

    index = index_location + "/" + index_name

    bowtieCMD = '/home/hadoop/apps/bowtie2-2.3.4.1-linux-x86_64/bowtie2 \
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
