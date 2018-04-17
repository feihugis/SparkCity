package edu.gmu.stc.raster.io

import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.io.hadoop.HdfsRangeReader
import geotrellis.util.StreamingByteReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by Fei Hu on 3/21/18.
  */
object GeoTiffReaderHelper {

  def readMultiBand(path: Path, hConf: Configuration): MultibandGeoTiff = {
    GeoTiffReader.readMultiband(
      StreamingByteReader(HdfsRangeReader(path, hConf))
    )
  }

  def readSingleBand(path: Path, hConf: Configuration): SinglebandGeoTiff = {
    GeoTiffReader.readSingleband(
      StreamingByteReader(HdfsRangeReader(path, hConf))
    )
  }



}
