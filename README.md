# Flint

This is the main repository of the Flint project for Amazon Web Services. Flint is a metagenomics profiling pipeline that is built on top of the [Apache Spark][1] framework, and is designed for fast real-time profiling of metagenomic samples against a large collection of reference genomes. Flint takes advantage of Spark's built-in parallelism and streaming engine architecture to quickly map reads against a large reference collection of bacterial genomes.

Our computational framework is primarily implemented using the MapReduce model, and deployed in a cluster launched using the [Elastic Map Reduce][2] service offered by AWS ([Amazon Web Services][3]). The cluster consists of multiple commodity worker machines (computational nodes), and in the current configuration of the cluster that we use, each worker machine consists of 15 GB of RAM, 8 vCPUs (a hyperthread of a single Intel Xeon core), and 100 GB of EBS disk storage. Each of the worker nodes will work in parallel to align the input sequencing DNA reads to a partitioned shard of the reference database; after the alignment step is completed, each worker node acts as a regular Spark executor node.

The current database for running Flint is version 41 from [Ensembl Bacteria][4], but we are currently working on the latest version of [RefSeq][5], which should be available this summer.

### Publications
**Valdes**, **Stebliankin**, **Narasimhan** (2019), Large Scale Microbiome Profiling in the Cloud, _ISMB 2019, in review_.



## How To Get Started

- [Download the Code][6] and follow the instructions on how to create an EMR cluster, setup the streaming source, and start Flint.

## Communication

- If you **found a bug**, open an issue and _please provide detailed steps to reliably reproduce it_.
- If you have **feature request**, open an issue.
- If you **would like to contribute**, please submit a pull request.

## Requirements
Flint is designed to run on [Apache Spark][7], but the current implementation is tuned for Amazon's EMR [Elastic Map Reduce][8]. The basic requrements for an [EMR][9] cluster are:

- [Apache Hadoop][10]
- [Apache Spark][11]
- [Ganglia][12]
- [Hue][13]
- [Hive][14]

The basic requirements for the worker nodes are:

- [Bowtie, v.2.3.4.1][15]
- [Python][16]
- [BioPython][17]
- [Pandas][18]
- [Fabric][19]
- [Boto3][20]
- [csv][21]
- [pathlib2][22]
- [shlex][23]
- [pickle][24]
- [subprocess][25]


### Bowtie2

[Bowtie][26] is required for the aligment step, and needs to be installed in all worker nodes of the Spark Cluster.  See the [Bowtie2 manual][27] for more information.

### Python Packages

The remaining requirements are python packages that Flint needs for a successfull run, please refer to the package's documentation for instructions and/or installation instructions.

## Contact

Contact [Camilo Valdes][28] for pull requests, bug reports, good jokes and coffee recipes.

### Maintainers

- [Camilo Valdes][29]


### Collaborators

- [Giri Narasimhan][30]
- [Vitalii Stebliankin][31]


## License

The software in this repository is available under the [MIT License][32].  See the [LICENSE][33] file for more information.

[1]:	https://spark.apache.org
[2]:	https://aws.amazon.com/emr/
[3]:	https://aws.amazon.com
[4]:	https://bacteria.ensembl.org/index.html
[5]:	https://www.ncbi.nlm.nih.gov/refseq/
[6]:	https://github.com/camilo-v/flint
[7]:	https://spark.apache.org
[8]:	https://aws.amazon.com/emr/
[9]:	https://aws.amazon.com/emr/
[10]:	https://hadoop.apache.org
[11]:	https://spark.apache.org
[12]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-ganglia.html
[13]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hue.html
[14]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive.html
[15]:	http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
[16]:	https://www.python.org
[17]:	https://biopython.org
[18]:	https://pandas.pydata.org
[19]:	http://www.fabfile.org
[20]:	https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
[21]:	https://docs.python.org/3/library/csv.html
[22]:	https://pypi.org/project/pathlib2/
[23]:	https://docs.python.org/3/library/shlex.html
[24]:	https://docs.python.org/3/library/pickle.html
[25]:	https://docs.python.org/2/library/subprocess.html
[26]:	http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
[27]:	http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml
[28]:	mailto:camilo@castflyer.com
[29]:	mailto:camilo@castflyer.com
[30]:	mailto:giri@cs.fiu.edu
[31]:	mailto:vsteb002@fiu.edu
[32]:	https://github.com/camilo-v/flint-aws/blob/master/LICENSE
[33]:	https://github.com/camilo-v/flint-aws/blob/master/LICENSE