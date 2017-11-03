#!/usr/bin/env bash
set -e

function usage {
    echo "Creates a Dataproc cluster with a Jupyter interface."
    echo "usage $0: [-h] [-n=name] [-b=bucket]"
    echo "    -h               display help"
    echo "    -n=name          name of cluster to create"
    echo "    -b=bucket        name of bucket in GCS for persistence"
    exit 1
}

for i in "$@"
do
    case $i in
        -n=*)
            CLUSTER_NAME="${i#*=}"
            shift
            ;;
        -b=*)
            BUCKET_NAME="${i#*=}"
            shift
            ;;
        -h)
            usage
            ;;
        *)
            ;;
    esac
done


[[ -z $CLUSTER_NAME ]] && usage
[[ -z $BUCKET_NAME ]] && usage

gcloud dataproc clusters create $CLUSTER_NAME \
    --metadata "JUPYTER_PORT=8124,JUPYTER_CONDA_PACKAGES=numpy" \
    --initialization-actions \
        gs://dataproc-initialization-actions/jupyter/jupyter.sh \
    --bucket $BUCKET_NAME \
    --num-workers 2
    #--worker-machine-type=n1-highcpu-8 \
    #--master-machine-type=n1-highcpu-8
