#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Downloads the Reuters dataset and prepares it for clustering
#
# To run:  change into the mahout directory and type:
#  examples/bin/build-reuters.sh

if [ "$1" = "--help" ] || [ "$1" = "--?" ]; then
  echo "This script clusters the Reuters data set using a variety of algorithms.  The data set is downloaded automatically."
  exit
fi

SCRIPT_PATH=${0%/*}
if [ "$0" != "$SCRIPT_PATH" ] && [ "$SCRIPT_PATH" != "" ]; then 
  cd $SCRIPT_PATH
fi

MAHOUT="../../bin/mahout"

if [ ! -e $MAHOUT ]; then
  echo "Can't find mahout driver in $MAHOUT, cwd `pwd`, exiting.."
  exit 1
fi

algorithm=( kmeans fuzzykmeans lda dirichlet minhash)
if [ -n "$1" ]; then
  choice=$1
else
  echo "Please select a number to choose the corresponding clustering algorithm"
  echo "1. ${algorithm[0]} clustering"
  echo "2. ${algorithm[1]} clustering"
  echo "3. ${algorithm[2]} clustering"
  echo "4. ${algorithm[3]} clustering"
  echo "5. ${algorithm[4]} clustering"
  read -p "Enter your choice : " choice
fi

echo "ok. You chose $choice and we'll use ${algorithm[$choice-1]} Clustering"
clustertype=${algorithm[$choice-1]} 

WORK_DIR=/tmp/mahout-work-${USER}
echo "creating work directory at ${WORK_DIR}"

mkdir -p ${WORK_DIR}

if [ ! -e ${WORK_DIR}/reuters-out-seqdir ]; then
  if [ ! -e ${WORK_DIR}/reuters-out ]; then
    if [ ! -e ${WORK_DIR}/reuters-sgm ]; then
      if [ ! -f ${WORK_DIR}/reuters21578.tar.gz ]; then
        echo "Downloading Reuters-21578"
        curl http://kdd.ics.uci.edu/databases/reuters21578/reuters21578.tar.gz -o ${WORK_DIR}/reuters21578.tar.gz
      fi
      mkdir -p ${WORK_DIR}/reuters-sgm
      echo "Extracting..."
      tar xzf ${WORK_DIR}/reuters21578.tar.gz -C ${WORK_DIR}/reuters-sgm
    fi
	
    $MAHOUT org.apache.lucene.benchmark.utils.ExtractReuters ${WORK_DIR}/reuters-sgm ${WORK_DIR}/reuters-out
  fi

  MAHOUT_LOCAL=true $MAHOUT seqdirectory -i ${WORK_DIR}/reuters-out -o ${WORK_DIR}/reuters-out-seqdir -c UTF-8 -chunk 5
fi

# we know reuters-out-seqdir exists on a local disk at
# this point, if we're running in clustered mode, 
# copy it up to hdfs
if [ "$HADOOP_HOME" != "" ] && [ "$MAHOUT_LOCAL" == "" ] ; then
  HADOOP="$HADOOP_HOME/bin/hadoop"
  if [ ! -e $HADOOP ]; then
    echo "Can't find hadoop in $HADOOP, exiting"
    exit 1
  fi

  set +e
  $HADOOP dfs -rmr ${WORK_DIR}/reuters-out-seqdir
  set -e
  $HADOOP dfs -put ${WORK_DIR}/reuters-out-seqdir ${WORK_DIR}/reuters-out-seqdir
fi

if [ "x$clustertype" == "xkmeans" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/reuters-out-seqdir/ \
    -o ${WORK_DIR}/reuters-out-seqdir-sparse-kmeans --maxDFPercent 85 --namedVector \
  && \
  $MAHOUT kmeans \
    -i ${WORK_DIR}/reuters-out-seqdir-sparse-kmeans/tfidf-vectors/ \
    -c ${WORK_DIR}/reuters-kmeans-clusters \
    -o ${WORK_DIR}/reuters-kmeans \
    -dm org.apache.mahout.common.distance.CosineDistanceMeasure \
    -x 10 -k 20 -ow --clustering \
  && \
  $MAHOUT clusterdump \
    -s ${WORK_DIR}/reuters-kmeans/clusters-*-final \
    -d ${WORK_DIR}/reuters-out-seqdir-sparse-kmeans/dictionary.file-0 \
    -dt sequencefile -b 100 -n 20 --evaluate -dm org.apache.mahout.common.distance.CosineDistanceMeasure \
    --pointsDir ${WORK_DIR}/reuters-kmeans/clusteredPoints
elif [ "x$clustertype" == "xfuzzykmeans" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/reuters-out-seqdir/ \
    -o ${WORK_DIR}/reuters-out-seqdir-sparse-fkmeans --maxDFPercent 85 --namedVector \
  && \
  $MAHOUT fkmeans \
    -i ${WORK_DIR}/reuters-out-seqdir-sparse-fkmeans/tfidf-vectors/ \
    -c ${WORK_DIR}/reuters-fkmeans-clusters \
    -o ${WORK_DIR}/reuters-fkmeans \
    -dm org.apache.mahout.common.distance.CosineDistanceMeasure \
    -x 10 -k 20 -ow -m 1.1 \
  && \
  $MAHOUT clusterdump \
    -s ${WORK_DIR}/reuters-fkmeans/clusters-*-final \
    -d ${WORK_DIR}/reuters-out-seqdir-sparse-fkmeans/dictionary.file-0 \
    -dt sequencefile -b 100 -n 20
elif [ "x$clustertype" == "xlda" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/reuters-out-seqdir/ \
    -o ${WORK_DIR}/reuters-out-seqdir-sparse-lda \
    -wt tf -seq -nr 3 --namedVector \
  && \
  $MAHOUT lda \
    -i ${WORK_DIR}/reuters-out-seqdir-sparse-lda/tf-vectors \
    -o ${WORK_DIR}/reuters-lda -k 20 -ow -x 20 \
  && \
  $MAHOUT ldatopics \
    -i ${WORK_DIR}/reuters-lda/state-20 \
    -d ${WORK_DIR}/reuters-out-seqdir-sparse-lda/dictionary.file-0 \
    -dt sequencefile
elif [ "x$clustertype" == "xdirichlet" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/reuters-out-seqdir/ \
    -o ${WORK_DIR}/reuters-out-seqdir-sparse-dirichlet  --maxDFPercent 85 --namedVector \
  && \
  $MAHOUT dirichlet \
    -i ${WORK_DIR}/reuters-out-seqdir-sparse-dirichlet/tfidf-vectors \
    -o ${WORK_DIR}/reuters-dirichlet -k 20 -ow -x 20 -a0 2 \
    -md org.apache.mahout.clustering.dirichlet.models.DistanceMeasureClusterDistribution \
    -mp org.apache.mahout.math.DenseVector \
    -dm org.apache.mahout.common.distance.CosineDistanceMeasure \
  && \
  $MAHOUT clusterdump \
    -s ${WORK_DIR}/reuters-dirichlet/clusters-*-final \
    -d ${WORK_DIR}/reuters-out-seqdir-sparse-dirichlet/dictionary.file-0 \
    -dt sequencefile -b 100 -n 20
elif [ "x$clustertype" == "xminhash" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/reuters-out-seqdir/ \
    -o ${WORK_DIR}/reuters-out-seqdir-sparse-minhash --maxDFPercent 85 --namedVector \
  && \
  $MAHOUT org.apache.mahout.clustering.minhash.MinHashDriver \
    -i ${WORK_DIR}/reuters-out-seqdir-sparse-minhash/tfidf-vectors \
    -o ${WORK_DIR}/reuters-minhash --overwrite
else 
  echo "unknown cluster type: $clustertype"
fi 
