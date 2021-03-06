#BSUB -J P-64-1
#BSUB -oo /path/to/logs/directory/bowtie2_log-64-1.txt
#BSUB -W 168:00
#BSUB -N
#BSUB -u "cvalde03@fiu.edu"
#BSUB -q PQ_narasimhan
#BSUB -x
#BSUB -n 16
#BSUB -R "span[ptile=16]"
#BSUB -M 62851307

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
#	The purpose of this script is to index a collection of bacterial genomes stored in a fasta file with the
#	bowtie2-build index builder program.
#
#   NOTES:
#   Please see the dependencies and/or assertions section below for any requirements.
#
#   DEPENDENCIES:
#
#       • Bowtie2
#
#
#	AUTHOR:	Camilo Valdes (cvalde03@fiu.edu)
#			Bioinformatics Research Group,
#			School of Computing and Information Sciences,
#			Florida International University (FIU)
#
# ---------------------------------------------------------------------------------------------------------------------

#   Load necessary modules for this HPC run.
module load python/2.7.5

echo ""
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Starting..."
echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"

ENSEMBL_VERSION="41"

PARTITION_MAJOR_NUMBER="64"
PARTITION_MINOR_NUMBER="1"

BASE_DIR="/path/to/output/directory/in/hpc/file_system/v"$ENSEMBL_VERSION"/partitions-"$PARTITION_MAJOR_NUMBER

FASTA_NAME="refseq_v"$ENSEMBL_VERSION
FASTA_FILE=$BASE_DIR"/"$PARTITION_MINOR_NUMBER"/"$FASTA_NAME".fasta"

OUTPUT_DIR=$BASE_DIR"/"$PARTITION_MINOR_NUMBER
mkdir -p $OUTPUT_DIR
cd $OUTPUT_DIR

INDEX_NAME=$FASTA_NAME

NUMBER_OF_THREADS=16

#
#	Bowtie2
#

BOWTIE_APP_PATH="/path/to/bowtie2/directory/2.3.4.1"

$BOWTIE_APP_PATH/bowtie2-build -f --threads $NUMBER_OF_THREADS $FASTA_FILE $INDEX_NAME


echo "[" `date '+%m/%d/%y %H:%M:%S'` "]"
echo "[" `date '+%m/%d/%y %H:%M:%S'` "] Done."
echo ""
