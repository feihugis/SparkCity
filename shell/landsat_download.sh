#! /bin/bash

LANDSAT_NAME="LC08_L1TP_015033_20170822_20170822_01_RT"
OUTPUT_DIR='./'
wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B3.TIF $OUTPUT_DIR
wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B4.TIF $OUTPUT_DIR
wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B5.TIF $OUTPUT_DIR
wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_BQA.TIF $OUTPUT_DIR
wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_B10.TIF $OUTPUT_DIR
wget http://landsat-pds.s3.amazonaws.com/c1/L8/015/033/$LANDSAT_NAME/"$LANDSAT_NAME"_MTL.txt $OUTPUT_DIR



# https://s3.amazonaws.com/geotrellis-sample-datasets/landsat/LC80140322014139LGN00.tar.bz
# tar xvfj LC80140322014139LGN00.tar.bz
