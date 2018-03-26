package edu.gmu.stc.raster.operation

import com.vividsolutions.jts.geom.{Geometry, MultiPolygon, Point, Polygon}
import geotrellis.raster.Tile
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector.{Extent}

/**
  * Created by Fei Hu on 3/21/18.
  */
object RasterOperation {

  def clipToPolygons(extent: Extent,
                     tiles: Array[Tile],
                     geometries: List[Geometry]): List[Geometry] = {

    if (tiles.exists(tile => tile.rows != tiles.head.rows || tile.cols != tiles.head.cols)) {
      throw new AssertionError("There are two input tiles with different size")
    }


    val cellWidth = (extent.xmax - extent.xmin) / tiles.head.cols
    val cellHeight = (extent.ymax - extent.ymin) / tiles.head.rows

    geometries.map {

      case point: Point => {
        val meanValues = tiles.map(
          tile => getPixelValueFromTile(point, tile, extent, cellWidth, cellHeight)
        )

        meanValues.foreach(meanValue => {
          point.setUserData(point.getUserData + "\t" + meanValue)
        })

        point
      }

      case polygon: Polygon => {
        val tileMeanValuePairs = tiles.map(tile => (tile, tile.polygonalMean(extent, polygon)))

        tileMeanValuePairs.foreach{
          case (tile, meanValue) => {
            if (meanValue.isNaN) {
              val point = polygon.getCentroid
              val value = getPixelValueFromTile(point, tile, extent, cellWidth, cellHeight)
              polygon.setUserData(polygon.getUserData + "\t" + value)
            } else {
              polygon.setUserData(polygon.getUserData + "\t" + meanValue)
            }
          }
        }

        polygon
      }

      case multiPolygon: MultiPolygon => {

        val tileMeanValuePairs = tiles.map(tile => (tile, tile.polygonalMean(extent, multiPolygon)))

        tileMeanValuePairs.foreach{
          case (tile, meanValue) => {
            if (meanValue.isNaN) {
              val point = multiPolygon.getCentroid
              val value = getPixelValueFromTile(point, tile, extent, cellWidth, cellHeight)
              multiPolygon.setUserData(multiPolygon.getUserData + "\t" + value)
            } else {
              multiPolygon.setUserData(multiPolygon.getUserData + "\t" + meanValue)
            }
          }
        }

        multiPolygon
      }

      case s: Any => {
        throw new AssertionError("Unconsistent geometry: " + s.getClass)
      }
    }
  }

  def getPixelValueFromTile(point: Point,
                            tile: Tile,
                            extent: Extent,
                            cellWidth: Double, cellHeight: Double): Double = {
    if (!extent.contains(point)) {
      Double.NaN
    } else {
      val x = (point.getX - extent.xmin) / cellWidth
      val y = (point.getY - extent.ymin) / cellHeight

      tile.getDouble(x.toInt, y.toInt)
    }
  }

  def getPixelValueFromTile(point: Point, tile: Tile, extent: Extent): Double = {
    if (!extent.contains(point)) {
      Double.NaN
    } else {
      val cellWidth = (extent.xmax - extent.xmin) / tile.cols
      val cellHeight = (extent.ymax - extent.ymin) / tile.rows

      getPixelValueFromTile(point, tile, extent, cellWidth, cellHeight)
    }
  }
}
