#!/bin/bash

pem_key="${PEM_KEY_NAME:-key_emr_cluster}"
cluster="${CLUSTER_NAME:-sparkify_emr_dcp}"
cluster_id=""

show_help() {
cat << EOF
Usage: ${0##*/} [-hv] [-f OUTFILE] [FILE]...
Create and manage EMR cluster
    -h                          display this help and exit
    -n  name of image/lambda    This input will be used to name the lambda and corresponding
                                docker image in the ECR
EOF
}

create_rol(){
aws emr create-default-roles
}

create_key(){
aws ec2 create-key-pair --key-name $pem_key | \
.python3 -c "import sys, json; print(json.load(sys.stdin)['KeyMaterial'])" > $pem_key.pem 
}

get_subnetid(){
vpcid=$(aws ec2 describe-vpcs --vpc-ids | \
python3 -c "import sys, json; [print(i['VpcId']) \
for i in json.load(sys.stdin)['Vpcs'] if i['IsDefault']]")
subnetid=$(aws ec2 describe-subnets \
--filters "Name=availability-zone,Values=us-east-1a" "Name=vpc-id,Values=$vpcid" | \
python3 -c "import sys, json; print(json.load(sys.stdin)['Subnets'][0]['SubnetId'])")
}

create_vpc(){
aws ec2 create-default-vpc
}

create_cluster(){
aws emr create-cluster --name $cluster \
--use-default-roles --release-label emr-5.28.0  \
--instance-count 3 --applications Name=Spark Name=Zeppelin  \
--ec2-attributes KeyName=$pem_key,SubnetId=$subnetid \
--instance-type m5.xlarge --log-uri s3://emrlogs/
}

cluster_status(){
aws emr describe-cluster --cluster-id $1
}

terminate_cluster(){
aws emr terminate-clusters --cluster-ids $1
}

# Read Params
OPTIND=1
while getopts :h?:ct:rkvgs: opt; do
    case $opt in
        h|\?)
            show_help
            exit 0
            ;;
        c)  get_subnetid
            create_cluster
            ;;
        g)  get_subnetid
            ;;
        k)  create_key 
            ;;
        r)  create_rol
            ;;
        s)  cluster_id=$OPTARG
            cluster_status $cluster_id 
            ;;
        t)  cluster_id=$OPTARG
            terminate_cluster $cluster_id 
            ;;
        v)  create_vpc
            ;;
        *)
            show_help >&2
            exit 1
            ;;
    esac
done

shift "$((OPTIND-1))"