package edu.gmu.stc.raster.io

import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.io.hadoop.HdfsRangeReader
import geotrellis.util.StreamingByteReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by Fei Hu on 3/21/18.
  */
object GeoTiffReaderHelper {

  def readMultiband(path: Path, hConf: Configuration): MultibandGeoTiff = {
    GeoTiffReader.readMultiband(
      StreamingByteReader(HdfsRangeReader(path, hConf))
    )
  }

}
