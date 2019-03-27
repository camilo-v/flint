#!/bin/bash

# ---------------------------------------------------------------------------------------------------------------------
#
#                                   	Bioinformatics Research Group
#										   http://biorg.cis.fiu.edu/
#                             		  Florida International University
#
#   This software is a "Camilo Valdes Work" under the terms of the United States Copyright Act.
#   Please cite the author(s) in any work or product based on this material.
#
#   OBJECTIVE:
#	The purpose of this script is to concatenate a collection of genome files downloaded from the Ensembl Bacteria
#	project website ("http://bacteria.ensembl.org"). The files to concatenate are FASTA files for each genome, and
#	the result will be one large FASTA file with all the sequences in them.
#	During concatenation the script applies several actions:
# 	1)	Clean the sequence headers of the FASTA files in the Ensembl Genome Index.
# 	    The sequence headers in their initial format are useless for mapping purposes as they do not have a format
# 	    that can be easily parsed by the Reducer steps in the main Flint MapReduce pipeline.
# 	2)	Create annotation file with all sequence headers.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#		* pandas
#		* Biopython
#
#
#	AUTHORS:	Camilo Valdes (cvalde03@fiu.edu), Vitalii Stebliankin (vsteb002@fiu.edu)
#				Bioinformatics Research Group,
#				School of Computing and Information Sciences,
#				Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

from Bio import SeqIO
from Bio.SeqRecord import SeqRecord
import os
import argparse
import pandas as pd
import time

print("concatenate.py starting..")
start = time.time()
# ---------------------------------------------- Variables initialization ----------------------------------------------
#
# 	Pick up the command line arguments
#
#
parser = argparse.ArgumentParser()
parser.add_argument( "--input_dir", required=True, type=str, help="Directory to FASTA files" )
parser.add_argument( "--db_version", required=True, type=str, help="Version of Ensembl Database" )
parser.add_argument( "--annotations_dir", required=True, type=str, help="Path to downloaded species_EnsemblBacteria.txt" )
parser.add_argument( "--output_dir", required=False, type=str, help="Output Directory" )

args = parser.parse_args()

input_dir    = args.input_dir
output_directory        = args.output_dir if args.output_dir is not None else "./out"
ensemblDatabaseVersion  = args.db_version
annotations_dir         = args.annotations_dir


annotation_file         = annotations_dir + "/" + "species_EnsemblBacteria.txt"
annotation_df           = pd.read_table(annotation_file, index_col=False)


# ---------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------- Output Files & Directories --------------------------------------------
#
# We'll check if the output directory exists ? either the default (current) or the requested one
#
if not os.path.exists( output_directory ):
    print(" Output directory does not exist.  Creating..." + "" )
    os.makedirs( output_directory )

outputFASTAFile = output_directory + "/" + "ensembl_bacterial_v" + ensemblDatabaseVersion + "-clean.fasta"
outputAssemblyAnnotation = annotations_dir + "/" + "assemblies_v" + ensemblDatabaseVersion + ".txt"

# Remove outputs if they already exist
if os.path.exists(outputFASTAFile):
    print("{} already exist. Removing {} ..".format(outputFASTAFile, outputFASTAFile))
    os.remove(outputFASTAFile)

if os.path.exists(outputAssemblyAnnotation):
    print("{} already exist. Removing {} ..".format(outputAssemblyAnnotation, outputAssemblyAnnotation))
    os.remove(outputAssemblyAnnotation)
# ---------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------- Functions --------------------------------------------------------

def write_to_file(list, file, delimiter='\t'):
    #create row to write:
    toWrite = ''
    for i, element in enumerate(list):
        if i != len(list) - 1:
            toWrite += str(element) + delimiter
        else:
            toWrite += str(element) + '\n'
    #append row to the file
    with open(file, 'a') as f:
        f.write(toWrite)
    return

def clean_fasta_records(fa):
    for record in SeqIO.parse(fa, "fasta"):


        sequence_attributes = record.description.split(" ")[2].split(":")

        # Replace unwanted characters to match assemblyID with annotation file entries:
        #assemblyID          = sequence_attributes[1].replace("._","-").replace("_","-").replace(".","-").replace("--","-")
        assemblyID          = sequence_attributes[1]
        genetic_structure   = sequence_attributes[0]
        sequenceID          = sequence_attributes[2].replace("_", "-")
        sequenceStart       = sequence_attributes[3]
        sequenceEnd         = sequence_attributes[4]
        scientific_name = fa.split("/")[-1].split(".")[0].lower()

        # Select all assemblies corresponding to our specie:
        species_df = annotation_df[annotation_df["species"] == scientific_name].reset_index(drop=True)
        if len(species_df)==1:
            gca_df=species_df
        else:
            # From those assemblies select one with assemblyID the same as ID from our sequence
        	gca_df = species_df[species_df["assembly"] == assemblyID].reset_index(drop=True)
        originalAssemblyID = assemblyID #keep track of original assemblyID
        if len(gca_df)==0:
            # change 5236_5_7 to 5236_5#7
            assemblyID_list = assemblyID.rsplit("_",1)
            assemblyID = "#".join(assemblyID_list)
            gca_df = species_df[species_df["assembly"] == assemblyID].reset_index(drop=True)

        if len(gca_df)>1:
            print(gca_df)
            raise IndexError("Failure to find unique GCA from assemblyID and species name for bacteria "+fa)

        elif len(gca_df)==1:
            gca = gca_df["assembly_accession"].apply(str)[0]
            gca = gca.split("_")[1]
            taxID = gca_df["taxonomy_id"].apply(str)[0]


            newFastaHeaderString = gca + "_" + sequenceID +"_"+ sequenceStart + "_" + sequenceEnd + "_" + taxID
            cleanRecordID = newFastaHeaderString

            cleanRecordSeq = record.seq

            #   The clean record's "description" field will be left blank because that information is contained in the
            #   new record's modified "id" field which has been modified with underscores.
            cleanRecord = SeqRecord(cleanRecordSeq, id=cleanRecordID, description="")

            with open(outputFASTAFile, "a") as outputHandleForCleanFastaFile:
                SeqIO.write(cleanRecord, outputHandleForCleanFastaFile, "fasta")
            # Write assembly to annotation file
            write_to_file([assemblyID, genetic_structure, sequenceID], outputAssemblyAnnotation, delimiter=".")
        else:
            print("Can't find GCA number for assembly {} of file {}".format(originalAssemblyID, fa))
            write_to_file([fa, originalAssemblyID], "not_annotated_assemblies.tsv")
# ---------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------ Main ---------------------------------------------------------

fasta_list = [os.path.join(input_dir,x) for x in os.listdir(input_dir)]
for fasta in fasta_list:
    clean_fasta_records(fasta)


print("Done concatenating. Computational time is {:,.2f} min.".format((time.time() - start)/60))

# ---------------------------------------------------------------------------------------------------------------------
