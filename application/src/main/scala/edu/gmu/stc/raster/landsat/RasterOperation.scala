package edu.gmu.stc.raster.landsat

import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector.Polygon

/**
  * Created by Fei Hu on 3/21/18.
  */
class RasterOperation {

  def clipToGrid(raster: MultibandGeoTiff, polygon: Polygon): Array[Double] = {
    raster.tile.polygonalMean(raster.extent, polygon)
  }
}
