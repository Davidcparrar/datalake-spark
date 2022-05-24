# Sparkify AWS Spark Data Lake

This Data Lake was created as a means to access the Sparkify data for analytical purposes.

## Project structure

The project has been created with the following structure:

```bash

├── README.md
├── dl_template.cfg
├── etl.py
└── iac_emrcluster.sh
```

- dl_template.cfg: Template for the configuration file. Fill in the missing information and rename the file to dl.cfg
- etl.py: Python script that loads the data from udacity S3 into a star schema in another S3 bucket.
- iac_emrcluster.sh: Bash utility that creates and deletes an EMR Cluster (IaC).

## Usage

### Cluster administration

Create the cluster

```bash
python iac_emrcluster.sh -c
```

> **Note**
> Make sure to write down the cluster id.

After a while check if the cluster is available by running the status command

```bash
python iac_emrcluster.sh -s $cluster_id
```

This will return a response and if the StateChangeReason is `Cluster ready to run steps.` the cluster is set to run the etl

The cluster can be terminated running:

```bash
python iac_emrcluster.sh -t $cluster_id
```

> **warning** 
> This will delete the cluster and all the information with it. After this point all non saved data will be lost and the creation command will be needed to recreate the cluster.** 

### Table creation


The data is ingested and inserted into the OLAP Schema.

- songplays
- users
- songs
- artists
- time 

To create the tables run ssh into the cluster

```bash
ssh -i $pem_key.pem user@url
```

Copy the script to the EMR cluster

```bash
scp -i $pem_key.pem etl.py user@url:/home/hadoop
```

and run following command. 

```bash
/usr/bin/spark-submit --master yarn ./etl.py
```

#### Star Schema

A star schema was implemented in order to make queries about the usage of the streaming app as simple as possible.