 # Flint

This is the ,ain repository of the Flint project for Amazon Web Services. Flint is a metagenomics profiling pipeline that is built on top of the [Apache Spark](https://spark.apache.org) framework, and is designed for fast real-time profiling of metagenomic samples against a large collection of reference genomes. Flint takes advantage of Spark's built-in parallelism and streaming engine architecture to quickly map reads against a large reference collection of bacterial genomes.

Our computational framework is primarily implemented using the MapReduce model, and deployed in a cluster launched using the [Elastic Map Reduce](https://aws.amazon.com/emr/) service offered by AWS ([Amazon Web Services](https://aws.amazon.com)). The cluster consists of multiple commodity worker machines (computational nodes), and in the current configuration of the cluster that we use, each worker machine cosists of 15 GB of RAM, 8 vCPUs (a hyperthread of a single Intel Xeon core), and 100 GB of EBS disk storage. Each of the worker nodes will work in parallel to align the input sequencing DNA reads to a partitioned shard of the reference database; after the alignment step is completed, each worker node acts as a regular Spark executor node.

The current database for running Flint is version 41 from [Ensembl Bacteria](https://bacteria.ensembl.org/index.html), but we are currently working on the latest version of [RefSeq](https://www.ncbi.nlm.nih.gov/refseq/), which should be available this summer.


## How To Get Started

- [Download the Code](https://github.com/camilo-v/flint) and follow the instructions on how to create an EMR cluster, setup the streaming source, and start Flint.

## Communication

- If you **found a bug**, open an issue and _please provide detailed steps to reliably reproduce it_.
- If you have **feature request**, open an issue.
- If you **would like to contribute**, please submit a pull request.

## Requirements
Flint is designed to run on [Apache Spark](https://spark.apache.org), but the current implementation is tuned for Amazon's EMR [Elastic Map Reduce](https://aws.amazon.com/emr/). The basic requrements for an [EMR](https://aws.amazon.com/emr/) cluster are:

- [Apache Hadoop](https://hadoop.apache.org)
- [Apache Spark](https://spark.apache.org)
- [Ganglia](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-ganglia.html)
- [Hue](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hue.html)
- [Hive](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive.html)

The basic requirements for the worker nodes are:

- [Python](https://www.python.org)
- [BioPython](https://biopython.org)
- [Pandas](https://pandas.pydata.org)
- [Fabric](http://www.fabfile.org)
- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

You will need a reasonably modern computer and operating system that can support the above.

### Requirement Package 1

[Some Package](http://somePackageURLgoesHere) is required for SOMETHING.  See the [package documentation](http://packageDocsURLgoesHere) for more information.

### Requirement Package 2

[Some Package](http://somePackageURLgoesHere) is required for SOMETHING.  See the [package documentation](http://packageDocsURLgoesHere) for more information.

### Requirement Package 3

[Some Package](http://somePackageURLgoesHere) is required for SOMETHING.  See the [package documentation](http://packageDocsURLgoesHere) for more information.  In addition, you will also need the following subpackages:

- [subpackage 1](http://someURLGoes.here/)
- [subpackage 2](http://someURLGoes.here/)
- [subpackage 3](http://someURLGoes.here/)
- [subpackage 4](http://someURLGoes.here/)
- [subpackage 5](http://someURLGoes.here/)
- [subpackage 6](http://someURLGoes.here/)



## Contact

Contact [Camilo Valdes](mailto:camilo@castflyer.com) for pull requests, bug reports, good jokes and coffee recipes.

### Maintainers

- [Camilo Valdes](mailto:camilo@castflyer.com)


### Collaborators

- [Giri Narasimhan](mailto:giri@cs.fiu.edu)
- [Vitalii Stebliankin](mailto:vsteb002@fiu.edu)


## License

The software in this repository is available under the [MIT License](https://github.com/camilo-v/flint-aws/blob/master/LICENSE).  See the [LICENSE](https://github.com/camilo-v/flint-aws/blob/master/LICENSE) file for more information.
