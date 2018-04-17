package edu.gmu.stc.raster.operation

import com.vividsolutions.jts.geom._
import edu.gmu.stc.analysis.ComputeSpectralIndex.logInfo
import edu.gmu.stc.analysis.MaskBandsRandGandNIR
import edu.gmu.stc.raster.io.GeoTiffReaderHelper
import edu.gmu.stc.raster.landsat.Calculations
import edu.gmu.stc.vector.VectorUtil
import edu.gmu.stc.vector.io.ShapeFileReaderHelper
import geotrellis.raster.{DoubleConstantNoDataCellType, Tile}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.geometry.jts.JTS
import org.apache.spark.internal.Logging

/**
  * Created by Fei Hu on 3/21/18.
  */
object RasterOperation extends  Logging {

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

  def computeSpectralIndex(hConf: Configuration,
                           landsatMulBandFilePath: Path,
                           indexNames: Array[String]): (Extent, Array[Tile]) = {
    val geotiff = GeoTiffReaderHelper.readMultiBand(landsatMulBandFilePath, hConf)
    val tile = geotiff.tile.convert(DoubleConstantNoDataCellType)

    val tiles = indexNames.map(indexName => {
      indexName.toLowerCase match {
        case "lst" => {
          val ndviTile = tile.combineDouble(
            MaskBandsRandGandNIR.R_BAND,
            MaskBandsRandGandNIR.NIR_BAND) {
            (r: Double, ir: Double) => Calculations.ndvi(r, ir)
          }

          val (ndvi_min, ndvi_max) = ndviTile.findMinMaxDouble

          tile.combineDouble(
            MaskBandsRandGandNIR.R_BAND,
            MaskBandsRandGandNIR.NIR_BAND,
            MaskBandsRandGandNIR.TIRS1_BAND) {
            (r: Double, nir: Double, tirs1: Double) => Calculations.lst(r, nir, tirs1, ndvi_min, ndvi_max)
          }
        }

        case "ndvi" => {
          tile.combineDouble(
            MaskBandsRandGandNIR.R_BAND,
            MaskBandsRandGandNIR.NIR_BAND) {
            (r: Double, ir: Double) => Calculations.ndvi(r, ir)
          }
        }

        case "ndwi" => {
          tile.combineDouble(
            MaskBandsRandGandNIR.G_BAND,
            MaskBandsRandGandNIR.NIR_BAND) {
            (g: Double, nir: Double) => Calculations.ndwi(g, nir)
          }
        }

        case "mndwi" => {
          tile.combineDouble(
            MaskBandsRandGandNIR.G_BAND,
            MaskBandsRandGandNIR.SWIR1_BAND) {
            (g: Double, swir1: Double) => Calculations.mndwi(g, swir1)
          }
        }

        case "ndbi" => {
          tile.combineDouble(
            MaskBandsRandGandNIR.SWIR1_BAND,
            MaskBandsRandGandNIR.NIR_BAND) {
            (swir1: Double, nir: Double) => Calculations.ndbi(swir1, nir)
          }
        }

        case "ndii" => {
          tile.combineDouble(
            MaskBandsRandGandNIR.R_BAND,
            MaskBandsRandGandNIR.TIRS1_BAND) {
            (r: Double, tirs1: Double) => Calculations.ndii(r, tirs1)
          }
        }

        case "ndisi" => {
          tile.combineDouble(
            MaskBandsRandGandNIR.TIRS1_BAND,
            MaskBandsRandGandNIR.G_BAND,
            MaskBandsRandGandNIR.NIR_BAND,
            MaskBandsRandGandNIR.SWIR1_BAND) {
            (tirs1: Double, g: Double, nir: Double, swir1: Double) => Calculations.ndisi(tirs1, g, nir, swir1)
          }
        }

        case default => {
          throw new AssertionError("Do not support this index: " + default)
        }
      }
    })

    (geotiff.extent, tiles)
  }

  def computeLSTAndNDVI(hConf: Configuration,
                        landsatMulBandFilePath: Path): (Extent, Tile, Tile) = {
    val geotiff = GeoTiffReaderHelper.readMultiBand(landsatMulBandFilePath, hConf)
    val tile = geotiff.tile.convert(DoubleConstantNoDataCellType)
    val ndviTile = tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
      MaskBandsRandGandNIR.NIR_BAND,
      MaskBandsRandGandNIR.TIRS1_BAND) {
      (r: Double, ir: Double, tirs: Double) => Calculations.ndvi(r, ir);
    }

    val (ndvi_min, ndvi_max) = ndviTile.findMinMaxDouble


    val lstTile = tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
      MaskBandsRandGandNIR.NIR_BAND,
      MaskBandsRandGandNIR.TIRS1_BAND) {
      (r: Double, ir: Double, tirs: Double) => Calculations.lst(r, ir, tirs, ndvi_min, ndvi_max);
    }

    //GeoTiff(lstTile, geotiff.extent, CRS.fromName("epsg:32618")).write("lst_test.tif")
    (geotiff.extent, lstTile, ndviTile)
  }

  def extractMeanValueFromTileByGeometry(hConfFile: String,
                                         rasterFile: String,
                                         rasterCRS: String,
                                         rasterLongitudeFirst: Boolean,
                                         vectorIndexTableName: String,
                                         vectorCRS: String,
                                         vectorLongitudeFirst: Boolean,
                                         indexNames: Array[String]
                                        ): List[Geometry] = {
    val hConf = new Configuration()
    hConf.addResource(new Path(hConfFile))

    val (rasterExtent, indexTiles) = RasterOperation.computeSpectralIndex(hConf, new Path(rasterFile), indexNames)

    val raster2osm_CRSTransform = VectorUtil.getCRSTransform(
      rasterCRS, rasterLongitudeFirst, vectorCRS, vectorLongitudeFirst
    )
    val osm2raster_CRSTransform = VectorUtil.getCRSTransform(
      vectorCRS, rasterLongitudeFirst, rasterCRS, vectorLongitudeFirst
    )

    val bbox = if (rasterCRS.equalsIgnoreCase(vectorCRS)) {
      new Envelope(rasterExtent.xmin, rasterExtent.xmax, rasterExtent.ymin, rasterExtent.ymax)
    } else {
      val envelope = new Envelope(rasterExtent.xmin, rasterExtent.xmax, rasterExtent.ymin, rasterExtent.ymax)
      JTS.transform(envelope, raster2osm_CRSTransform)
    }

    val polygons = ShapeFileReaderHelper.read(
      hConf,
      vectorIndexTableName,
      bbox.getMinX,
      bbox.getMinY,
      bbox.getMaxX,
      bbox.getMaxY,
      hasAttribute = true)
      .map(geometry => JTS.transform(geometry, osm2raster_CRSTransform))

    val polygonsWithIndices = RasterOperation
      .clipToPolygons(rasterExtent, indexTiles, polygons)
      .filter(g => !g.getUserData.toString.contains("NaN"))
      .map(geometry => JTS.transform(geometry, raster2osm_CRSTransform))

    logInfo("*********** Number of Polygons: " + polygonsWithIndices.size)
    logInfo("*********** Number of Input Polygons: " + polygons.size)

    polygonsWithIndices
  }
}
