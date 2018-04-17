#! /bin/bash

#LANDSAT_NAME="LC08_L1TP_015033_20170822_20170822_01_RT"
#OUTPUT_DIR="/var/lib/hadoop-hdfs/SparkCity/data/$LANDSAT_NAME"
#mkdir -p $OUTPUT_DIR

download_landsat()
{
    LANDSAT_NAME=$1
    OUTPUT_DIR="$2/$1"
    mkdir -p $OUTPUT_DIR

    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B1.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B2.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B3.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B4.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B5.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B6.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B7.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B8.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B9.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B10.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B11.TIF -P $OUTPUT_DIR
    wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_BQA.TIF -P $OUTPUT_DIR
}

while read p; do
    echo $p
    download_landsat $p ../data/
done < "landsat_files.txt"



