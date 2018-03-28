package edu.gmu.stc.analysis

import com.vividsolutions.jts.geom._
import edu.gmu.stc.analysis.ComputeLST_LowAccuracy.ComputeLSTConfig
import edu.gmu.stc.raster.io.GeoTiffReaderHelper
import edu.gmu.stc.raster.landsat.{Calculations, MaskBandsRandGandNIR}
import edu.gmu.stc.raster.operation.RasterOperation
import edu.gmu.stc.vector.VectorUtil
import edu.gmu.stc.vector.io.ShapeFileReaderHelper
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import edu.gmu.stc.vector.sourcedata.OSMAttributeUtil
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{DoubleConstantNoDataCellType, Tile}
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.geotools.geometry.jts.JTS

import scala.collection.JavaConverters._

/**
  * Created by Fei Hu on 3/21/18.
  */
object ComputeLST_HighAccuracy extends Logging{

  def computeIndex(hConf: Configuration,
                   landsatMulBandFilePath: Path,
                   indexNames: Array[String]): (Extent, Array[Tile]) = {
    val geotiff = GeoTiffReaderHelper.readMultiband(landsatMulBandFilePath, hConf)
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
    val geotiff = GeoTiffReaderHelper.readMultiband(landsatMulBandFilePath, hConf)
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

  /*def extractLSTMeanValueFromTileByGeometry(hConfFile: String,
                                            lstTile: Tile,
                                            rasterExtent: Extent,
                                            rasterCRS: String,
                                            longitudeFirst: Boolean,
                                            vectorIndexTableName: String, vectorCRS: String
                                           ): List[Geometry] = {
    val hConf = new Configuration()
    hConf.addResource(new Path(hConfFile))

    val raster2osm_CRSTransform = VectorUtil.getCRSTransform(rasterCRS, vectorCRS, longitudeFirst)
    val osm2raster_CRSTransform = VectorUtil.getCRSTransform(vectorCRS, rasterCRS, longitudeFirst)

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

    val polygonsWithLST = RasterOperation
      .clipToPolygons(rasterExtent, Array(lstTile), polygons)
      .filter(g => !g.getUserData.toString.contains("NaN"))
      .map(geometry => JTS.transform(geometry, raster2osm_CRSTransform))

    logInfo("*********** Number of Polygons: " + polygonsWithLST.size)
    logInfo("*********** Number of Input Polygons: " + polygons.size)

    polygonsWithLST
  }*/

  def extractLSTMeanValueFromTileByGeometry(hConfFile: String,
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

    val (rasterExtent, indexTiles) = computeIndex(hConf, new Path(rasterFile), indexNames)

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

  case class ComputeLSTConfig(hConfFile: String,
                              rasterFile: String,
                              rasterCRS: String,
                              rasterLongitudeFirst: Boolean,
                              vectorIndexTableName: String,
                              vectorCRS: String,
                              vectorLongitudeFirst: Boolean,
                              osmLayerName: String,
                              outputShpPath: String)

  //TODO: optimize the input parameters. For example, support the input of tile which can be reused
  // by other OSM layers
  def addLSTToOSMLayer(computeLSTConfig: ComputeLSTConfig, indexNames: Array[String]): Unit = {
    val polygonsWithLST = extractLSTMeanValueFromTileByGeometry(computeLSTConfig.hConfFile,
      computeLSTConfig.rasterFile,
      computeLSTConfig.rasterCRS,
      computeLSTConfig.rasterLongitudeFirst,
      computeLSTConfig.vectorIndexTableName,
      computeLSTConfig.vectorCRS,
      computeLSTConfig.vectorLongitudeFirst,
      indexNames)

    val attributeSchema = OSMAttributeUtil.getLayerAtrributes(computeLSTConfig.osmLayerName)

    GeometryReaderUtil.saveAsShapefile(
      computeLSTConfig.outputShpPath,
      computeLSTConfig.vectorCRS,
      classOf[Polygon],
      polygonsWithLST.asJava,
      attributeSchema)

    GeometryReaderUtil.saveDbfAsCSV(
      polygonsWithLST.asJava,
      attributeSchema,
      computeLSTConfig.outputShpPath.replace(".shp", ".csv"))
  }

  def main(args: Array[String]): Unit = {

    val buildingsConfig = ComputeLSTConfig(
      hConfFile = "config/conf_lst_va.xml",
      rasterFile = "data/r-g-nir-tirs1-swir1.tif",
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = "gis_osm_buildings_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "buildings_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_buildings/lst_va_buildings.shp"
    )

    val landuseConfig = ComputeLSTConfig(
      hConfFile = "config/conf_lst_va.xml",
      rasterFile = "data/r-g-nir-tirs1-swir1.tif",
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = "gis_osm_landuse_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "landuse_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_landuse/lst_va_landuse.shp"
    )

    val poisConfig = ComputeLSTConfig(
      hConfFile = "config/conf_lst_va.xml",
      rasterFile = "data/r-g-nir-tirs1-swir1.tif",
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = "gis_osm_pois_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "pois_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_pois/lst_va_pois.shp"
    )

    val trafficConfig = ComputeLSTConfig(
      hConfFile = "config/conf_lst_va.xml",
      rasterFile = "data/r-g-nir-tirs1-swir1.tif",
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = "gis_osm_traffic_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "traffic_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_traffic/lst_va_traffic.shp"
    )

    val waterConfig = ComputeLSTConfig(
      hConfFile = "config/conf_lst_va.xml",
      rasterFile = "data/r-g-nir-tirs1-swir1.tif",
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = "gis_osm_water_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "water_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_water/lst_va_water.shp"
    )

    val blockConfig = ComputeLSTConfig(
      hConfFile = "config/conf_lst_va.xml",
      rasterFile = "data/r-g-nir-tirs1-swir1.tif",
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = "cb_2016_51_bg_500k",
      vectorCRS = "epsg:4269",
      vectorLongitudeFirst = true,
      osmLayerName = "block_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_block/lst_va_block.shp"
    )

    val indexNames = Array("lst", "ndvi", "ndwi", "ndbi", "ndii", "mndwi", "ndisi")
    val configs = Array(buildingsConfig, landuseConfig, poisConfig, trafficConfig, waterConfig, blockConfig)
    configs.foreach(config => {
      addLSTToOSMLayer(config, indexNames)
      logInfo("Finished the processing of " + config.vectorIndexTableName)
    })
  }
}
