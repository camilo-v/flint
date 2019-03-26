# Flint
Welcome to Flint. Flint is a metagenomics profiling pipeline that is built on top of the [Apache Spark]()[1]() framework, and is designed for fast real-time profiling of metagenomic samples against a large collection of reference genomes. Flint takes advantage of Spark's built-in parallelism and streaming engine architecture to quickly map reads against a large reference collection of bacterial genomes.

Our computational framework is primarily implemented using Spark’s MapReduce model, and deployed in a cluster launched using the [Elastic Map Reduce]()[2]() (EMR) service offered by AWS ([Amazon Web Services]()[3]()). The cluster consists of multiple commodity worker machines (computational nodes), and in the current configuration of the cluster that we use, each worker machine consists of 15 GB of RAM, 8 vCPUs (a hyperthread of a single Intel Xeon core), and 100 GB of EBS disk storage. Each of the worker nodes will work in parallel to align the input sequencing DNA reads to a partitioned shard of the reference database; after the alignment step is completed, each worker node acts as a regular Spark executor node.

## Installation
Flint runs on Spark, but the current version is tuned for Amazon’s EMR service. While the Amazon-specific code is minimal, we are not yet supporting Spark clusters outside of EMR, although this support is coming.

A Flint project consists of multiple pieces (outlined below), and you will need to stage the genome reference assets and machine configurations before launching a cluster. The steps for launching a Flint cluster are three (3):
1. . **Asset Staging**.
	- Upload the bacterial reference genomes to an accessible data bucket in Amazon’s S3 storage, along with configuration scripts for the cluster.
2. **Launch an EMR cluster**.
	- Launch an EMR cluster that will be configured with bootstrap actions and steps that you staged in the previous step.
3. **Flint source code **.
	- Upload the main Flint python script, along with utilities for copying the reference shards into each cluster node, and a template of the `spark-submit` resource file, into the cluster’s master node.

The following section(s) contain details on each of the above steps. If you have any questions, comments, and/or queries, please get in touch with the project’s maintainer linked at the footer of this page.

## Asset Staging
This section involves two (2) parts: the upload of the bacterial genomes into a location that is accessible by the worker machines in the cluster, and the upload of two configuration scripts that the EMR launch procedure will use to provision the cluster.

### Bacterial Genomes

### Cluster Configuration


## Launch an EMR Cluster
Flint is currently optimized for running on AWS, and on the EMR service specifically. You will need an AWS account to launch an EMR cluster, so familiarity with AWS and EMR is ideal. If you are not familiar with AWS, you can start with [this tutorial][7]. 

Please note that the following steps, and screenshots, all assume that you have successfully signed into your [AWS Management Console][8].

![AWS Console][image-1]

In the management console main page, you can search for “EMR”; this will take you to the EMR dashboard, which will load with the clusters you have created (if any). Click on the “**Create Cluster**” button to start.

![EMR Dashboard][image-2]

This will take you to the “**Create Cluster - Quick Options**” configuration page. You can launch a cluster in this page right away, but we’ll go ahead and click on “**Go to advanced options**” link to launch a cluster with some specific changes required for Flint.

![Advanced Options][image-3]




### Publication
Flint can be referenced by using the citation:

**Valdes**, **Stebliankin**, **Narasimhan** (2019), Large Scale Microbiome Profiling in the Cloud, _ISMB 2019, in review_.

[7]:	https://aws.amazon.com/getting-started/ "Getting Started with AWS"
[8]:	https://aws.amazon.com/console/ "AWS Management Console"

[image-1]:	images/aws-console.png "AWS Console"
[image-2]:	images/emr-1.png "EMR Dashboard"
[image-3]:	images/emr-2.png "Advanced Options"