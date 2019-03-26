# Flint

This is the main repository of the Flint project for Amazon Web Services. Flint is a metagenomics profiling pipeline that is built on top of the [Apache Spark][1] framework, and is designed for fast real-time profiling of metagenomic samples against a large collection of reference genomes. Flint takes advantage of Spark's built-in parallelism and streaming engine architecture to quickly map reads against a large reference collection of bacterial genomes.

Our computational framework is primarily implemented using the MapReduce model, and deployed in a cluster launched using the [Elastic Map Reduce][2] service offered by AWS ([Amazon Web Services][3]). The cluster consists of multiple commodity worker machines (computational nodes), and in the current configuration of the cluster that we use, each worker machine consists of 15 GB of RAM, 8 vCPUs (a hyperthread of a single Intel Xeon core), and 100 GB of EBS disk storage. Each of the worker nodes will work in parallel to align the input sequencing DNA reads to a partitioned shard of the reference database; after the alignment step is completed, each worker node acts as a regular Spark executor node.

The current database for running Flint is version 41 from [Ensembl Bacteria][4], but we are currently working on the latest version of [RefSeq][5], which should be available this summer.

### Publications
**Valdes**, **Stebliankin**, **Narasimhan** (2019), Large Scale Microbiome Profiling in the Cloud, _ISMB 2019, in review_.



## How To Get Started

- [Download the Code][6] and [follow the instructions][7] on how to create an EMR cluster, setup the streaming source, and start Flint.
- Detailed instructions, as well as a manual and reference, can be found at the [project’s website][8].

## Communication

- If you **found a bug**, open an issue and _please provide detailed steps to reliably reproduce it_.
- If you have **feature request**, open an issue.
- If you **would like to contribute**, please submit a pull request.

## Requirements
Flint is designed to run on [Apache Spark][9], but the current implementation is tuned for Amazon's EMR [Elastic Map Reduce][10]. The basic requrements for an [EMR][11] cluster are:

- [Apache Hadoop][12]
- [Apache Spark][13]
- [Ganglia][14]
- [Hue][15]
- [Hive][16]

The basic requirements for the worker nodes are:

- [Bowtie, v.2.3.4.1][17]
- [Python][18]
- [BioPython][19]
- [Pandas][20]
- [Fabric][21]
- [Boto3][22]
- [csv][23]
- [pathlib2][24]
- [shlex][25]
- [pickle][26]
- [subprocess][27]


### Bowtie2

[Bowtie][28] is required for the aligment step, and needs to be installed in all worker nodes of the Spark Cluster.  See the [Bowtie2 manual][29] for more information.

### Python Packages

The remaining requirements are python packages that Flint needs for a successfull run, please refer to the package's documentation for instructions and/or installation instructions.

## Contact

Contact [Camilo Valdes][30] for pull requests, bug reports, good jokes and coffee recipes.

### Maintainers

- [Camilo Valdes][31]


### Collaborators

- [Giri Narasimhan][32]
- [Vitalii Stebliankin][33]


## License

The software in this repository is available under the [MIT License][34].  See the [LICENSE][35] file for more information.

[1]:	https://spark.apache.org
[2]:	https://aws.amazon.com/emr/
[3]:	https://aws.amazon.com
[4]:	https://bacteria.ensembl.org/index.html
[5]:	https://www.ncbi.nlm.nih.gov/refseq/
[6]:	https://github.com/camilo-v/flint
[7]:	https://camilo-v.github.io/flint/ "Manual"
[8]:	https://camilo-v.github.io/flint/ "Flint Project"
[9]:	https://spark.apache.org
[10]:	https://aws.amazon.com/emr/
[11]:	https://aws.amazon.com/emr/
[12]:	https://hadoop.apache.org
[13]:	https://spark.apache.org
[14]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-ganglia.html
[15]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hue.html
[16]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive.html
[17]:	http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
[18]:	https://www.python.org
[19]:	https://biopython.org
[20]:	https://pandas.pydata.org
[21]:	http://www.fabfile.org
[22]:	https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
[23]:	https://docs.python.org/3/library/csv.html
[24]:	https://pypi.org/project/pathlib2/
[25]:	https://docs.python.org/3/library/shlex.html
[26]:	https://docs.python.org/3/library/pickle.html
[27]:	https://docs.python.org/2/library/subprocess.html
[28]:	http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
[29]:	http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml
[30]:	mailto:camilo@castflyer.com
[31]:	mailto:camilo@castflyer.com
[32]:	mailto:giri@cs.fiu.edu
[33]:	mailto:vsteb002@fiu.edu
[34]:	https://github.com/camilo-v/flint-aws/blob/master/LICENSE
[35]:	https://github.com/camilo-v/flint-aws/blob/master/LICENSE