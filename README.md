# Flint

This is the main repository of the Flint project for Amazon Web Services. Flint is a metagenomics profiling pipeline that is built on top of the [Apache Spark][1] framework, and is designed for fast real-time profiling of metagenomic samples against a large collection of reference genomes. Flint takes advantage of Spark's built-in parallelism and streaming engine architecture to quickly map reads against a large reference collection of bacterial genomes.

Our computational framework is primarily implemented using the MapReduce model, and deployed in a cluster launched using the [Elastic Map Reduce][2] service offered by AWS ([Amazon Web Services][3]). The cluster consists of multiple commodity worker machines (computational nodes), and in the current configuration of the cluster that we use, each worker machine consists of 15 GB of RAM, 8 vCPUs (a hyperthread of a single Intel Xeon core), and 100 GB of EBS disk storage. Each of the worker nodes will work in parallel to align the input sequencing DNA reads to a partitioned shard of the reference database; after the alignment step is completed, each worker node acts as a regular Spark executor node.

The current database for running Flint is version 41 from [Ensembl Bacteria][4], but we are currently working on the latest version of [RefSeq][5], which should be available this summer.

### Publications
**Valdes**, **Stebliankin**, **Narasimhan** (2019), Large Scale Microbiome Profiling in the Cloud, _ISMB 2019, in review_.



## How To Get Started

- [Download the Code][6] and [follow the instructions][7] on how to create an EMR cluster, setup the streaming source, and start Flint.
- [Instructions][8], as well as a [manual and reference][9], can be found at the [project’s website][10].

## Communication

- If you **found a bug**, open an issue and _please provide detailed steps to reliably reproduce it_.
- If you have **feature request**, open an issue.
- If you **would like to contribute**, please submit a pull request.

## Requirements
Flint is designed to run on [Apache Spark][11], but the current implementation is tuned for Amazon's EMR [Elastic Map Reduce][12]. The basic requrements for an [EMR][13] cluster are:

- [Apache Hadoop][14]
- [Apache Spark][15]
- [Ganglia][16]
- [Hue][17]
- [Hive][18]

The basic requirements for the worker nodes are:

- [Bowtie, v.2.3.4.1][19]
- [Python][20]
- [BioPython][21]
- [Pandas][22]
- [Fabric][23]
- [Boto3][24]
- [csv][25]
- [pathlib2][26]
- [shlex][27]
- [pickle][28]
- [subprocess][29]


### Bowtie2

[Bowtie][30] is required for the aligment step, and needs to be installed in all worker nodes of the Spark Cluster.  See the [Bowtie2 manual][31] for more information.

### Python Packages

The remaining requirements are python packages that Flint needs for a successfull run, please refer to the package's documentation for instructions and/or installation instructions.

## Contact

Contact [Camilo Valdes][32] for pull requests, bug reports, good jokes and coffee recipes.

### Maintainers

- [Camilo Valdes][33]


### Collaborators

- [Giri Narasimhan][34]
- [Vitalii Stebliankin][35]


## License

The software in this repository is available under the [MIT License][36].  See the [LICENSE][37] file for more information.

[1]:	https://spark.apache.org
[2]:	https://aws.amazon.com/emr/
[3]:	https://aws.amazon.com
[4]:	https://bacteria.ensembl.org/index.html
[5]:	https://www.ncbi.nlm.nih.gov/refseq/
[6]:	https://github.com/camilo-v/flint
[7]:	https://camilo-v.github.io/flint/ "Manual"
[8]:	https://camilo-v.github.io/flint/ "Instructions"
[9]:	https://camilo-v.github.io/flint/ "Manual"
[10]:	https://camilo-v.github.io/flint/ "Flint Project"
[11]:	https://spark.apache.org
[12]:	https://aws.amazon.com/emr/
[13]:	https://aws.amazon.com/emr/
[14]:	https://hadoop.apache.org
[15]:	https://spark.apache.org
[16]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-ganglia.html
[17]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hue.html
[18]:	https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive.html
[19]:	http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
[20]:	https://www.python.org
[21]:	https://biopython.org
[22]:	https://pandas.pydata.org
[23]:	http://www.fabfile.org
[24]:	https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
[25]:	https://docs.python.org/3/library/csv.html
[26]:	https://pypi.org/project/pathlib2/
[27]:	https://docs.python.org/3/library/shlex.html
[28]:	https://docs.python.org/3/library/pickle.html
[29]:	https://docs.python.org/2/library/subprocess.html
[30]:	http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
[31]:	http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml
[32]:	mailto:camilo@castflyer.com
[33]:	mailto:camilo@castflyer.com
[34]:	mailto:giri@cs.fiu.edu
[35]:	mailto:vsteb002@fiu.edu
[36]:	https://github.com/camilo-v/flint-aws/blob/master/LICENSE
[37]:	https://github.com/camilo-v/flint-aws/blob/master/LICENSE