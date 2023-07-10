#!/bin/bash

FILE=./src/main/resources/training.csv

# Start training with KMeans
spark-submit --class es.dmr.uimp.clustering.KMeansClusterInvoices --master local[4] target/scala-2.11/anomalyDetection-assembly-1.0.jar ${FILE} ./clustering ./threshold

# Start training with KMeans
spark-submit --class es.dmr.uimp.clustering.BisectingKMeansClusterInvoices --master local[4] target/scala-2.11/anomalyDetection-assembly-1.0.jar ${FILE} ./clustering_bisect ./threshold_bisect