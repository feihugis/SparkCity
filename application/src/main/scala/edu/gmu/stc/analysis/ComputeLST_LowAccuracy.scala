package edu.gmu.stc.analysis

import com.vividsolutions.jts.geom._
import edu.gmu.stc.raster.io.GeoTiffReaderHelper
import edu.gmu.stc.raster.landsat.{Calculations, MaskBandsRandGandNIR}
import edu.gmu.stc.raster.operation.RasterOperation
import edu.gmu.stc.vector.VectorUtil
import edu.gmu.stc.vector.io.ShapeFileReaderHelper
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import edu.gmu.stc.vector.sourcedata.OSMAttributeUtil
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
object ComputeLST_LowAccuracy extends Logging{

  def computeLST(hConf: Configuration,
                 landsatMulBandFilePath: Path): (Extent, Tile) = {
    val geotiff = GeoTiffReaderHelper.readMultiband(landsatMulBandFilePath, hConf)
    val tile = geotiff.tile.convert(DoubleConstantNoDataCellType)
    val (ndvi_min, ndvi_max) = tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
      MaskBandsRandGandNIR.NIR_BAND,
      MaskBandsRandGandNIR.TIRS_BAND) {
      (r: Double, ir: Double, tirs: Double) => Calculations.ndvi(r, ir);
    }.findMinMaxDouble


    val lstTile = tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
      MaskBandsRandGandNIR.NIR_BAND,
      MaskBandsRandGandNIR.TIRS_BAND) {
      (r: Double, ir: Double, tirs: Double) => Calculations.lst(r, ir, tirs, ndvi_min, ndvi_max);
    }

    (geotiff.extent, lstTile)
  }

  def extractLSTMeanValueFromTileByGeometry(hConfFile: String,
                                            lstTile: Tile,
                                            rasterExtent: Extent,
                                            rasterCRS: String,
                                            longitudeFirst: Boolean,
                                            vectorIndexTableName: String, vectorCRS: String
                                           ): List[Geometry] = {
    val hConf = new Configuration()
    hConf.addResource(new Path(hConfFile))

    val bbox = if (rasterCRS.equalsIgnoreCase(vectorCRS)) {
      new Envelope(rasterExtent.xmin, rasterExtent.xmax, rasterExtent.ymin, rasterExtent.ymax)
    } else {
      val crsTransform = VectorUtil.getCRSTransform(rasterCRS, vectorCRS, longitudeFirst)
      val envelope = new Envelope(rasterExtent.xmin, rasterExtent.xmax, rasterExtent.ymin, rasterExtent.ymax)
      JTS.transform(envelope, crsTransform)
    }

    val polygons = ShapeFileReaderHelper.read(
      hConf,
      vectorIndexTableName,
      bbox.getMinX,
      bbox.getMinY,
      bbox.getMaxX,
      bbox.getMaxY,
      true)

    val polygonsWithLST = RasterOperation
      .clipToPolygons(bbox, lstTile, polygons)
      .filter(g => !g.getUserData.toString.contains("NaN"))

    logInfo("*********** Number of Polygons: " + polygonsWithLST.size)
    logInfo("*********** Number of Input Polygons: " + polygons.size)

    polygonsWithLST
  }

  def extractLSTMeanValueFromTileByGeometry(hConfFile: String,
                                         rasterFile: String, rasterCRS: String,
                                         longitudeFirst: Boolean,
                                         vectorIndexTableName: String, vectorCRS: String
                                        ): List[Geometry] = {
    val hConf = new Configuration()
    hConf.addResource(new Path(hConfFile))

    val (rasterExtent, lstTile) = computeLST(hConf, new Path(rasterFile))

    val bbox = if (rasterCRS.equalsIgnoreCase(vectorCRS)) {
      new Envelope(rasterExtent.xmin, rasterExtent.xmax, rasterExtent.ymin, rasterExtent.ymax)
    } else {
      val crsTransform = VectorUtil.getCRSTransform(rasterCRS, vectorCRS, longitudeFirst)
      val envelope = new Envelope(rasterExtent.xmin, rasterExtent.xmax, rasterExtent.ymin, rasterExtent.ymax)
      JTS.transform(envelope, crsTransform)
    }

    val polygons = ShapeFileReaderHelper.read(
      hConf,
      vectorIndexTableName,
      bbox.getMinX,
      bbox.getMinY,
      bbox.getMaxX,
      bbox.getMaxY,
      true)

    val polygonsWithLST = RasterOperation
      .clipToPolygons(bbox, lstTile, polygons)
      .filter(g => !g.getUserData.toString.contains("NaN"))

    logInfo("*********** Number of Polygons: " + polygonsWithLST.size)
    logInfo("*********** Number of Input Polygons: " + polygons.size)

    polygonsWithLST
  }

  case class ComputeLSTConfig(hConfFile: String,
                              rasterFile: String,
                              rasterCRS: String,
                              longitudeFirst: Boolean,
                              vectorIndexTableName: String,
                              vectorCRS: String,
                              osmLayerName: String,
                              outputShpPath: String)

  //TODO: optimize the input parameters. For example, support the input of tile which can be reused
  // by other OSM layers
  def addLSTToOSMLayer(computeLSTConfig: ComputeLSTConfig): Unit = {
    val polygonsWithLST = extractLSTMeanValueFromTileByGeometry(computeLSTConfig.hConfFile,
      computeLSTConfig.rasterFile,
      computeLSTConfig.rasterCRS,
      computeLSTConfig.longitudeFirst,
      computeLSTConfig.vectorIndexTableName,
      computeLSTConfig.vectorCRS)

    val attributeSchema = OSMAttributeUtil.getLayerAtrributes(computeLSTConfig.osmLayerName)

    GeometryReaderUtil.saveAsShapefile(computeLSTConfig.outputShpPath,
      computeLSTConfig.vectorCRS,
      classOf[Polygon],
      polygonsWithLST.asJava,
      attributeSchema)
  }

  def main(args: Array[String]): Unit = {

    val buildingsConfig = ComputeLSTConfig(
      hConfFile = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      rasterFile = "/Users/feihu/Documents/GitHub/SparkCity/data/r-g-nir.tif",
      rasterCRS = "epsg:32618",
      longitudeFirst = true,
      vectorIndexTableName = "gis_osm_buildings_a_free_1",
      vectorCRS = "epsg:4326",
      osmLayerName = "buildings_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_buildings_va/lst_buildings_va_lowAcc.shp"
    )

    val landuseConfig = ComputeLSTConfig(
      hConfFile = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      rasterFile = "/Users/feihu/Documents/GitHub/SparkCity/data/r-g-nir.tif",
      rasterCRS = "epsg:32618",
      longitudeFirst = true,
      vectorIndexTableName = "gis_osm_landuse_a_free_1",
      vectorCRS = "epsg:4326",
      osmLayerName = "landuse_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_landuse_va/lst_landuse_va_lowAcc.shp"
    )

    val poisConfig = ComputeLSTConfig(
      hConfFile = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      rasterFile = "/Users/feihu/Documents/GitHub/SparkCity/data/r-g-nir.tif",
      rasterCRS = "epsg:32618",
      longitudeFirst = true,
      vectorIndexTableName = "gis_osm_pois_a_free_1",
      vectorCRS = "epsg:4326",
      osmLayerName = "pois_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_pois_va/lst_pois_va_lowAcc.shp"
    )

    val trafficConfig = ComputeLSTConfig(
      hConfFile = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      rasterFile = "/Users/feihu/Documents/GitHub/SparkCity/data/r-g-nir.tif",
      rasterCRS = "epsg:32618",
      longitudeFirst = true,
      vectorIndexTableName = "gis_osm_traffic_a_free_1",
      vectorCRS = "epsg:4326",
      osmLayerName = "traffic_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_traffic_va/lst_traffic_va_lowAcc.shp"
    )

    val waterConfig = ComputeLSTConfig(
      hConfFile = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      rasterFile = "/Users/feihu/Documents/GitHub/SparkCity/data/r-g-nir.tif",
      rasterCRS = "epsg:32618",
      longitudeFirst = true,
      vectorIndexTableName = "gis_osm_water_a_free_1",
      vectorCRS = "epsg:4326",
      osmLayerName = "water_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_water_va/lst_water_va_lowAcc.shp"
    )

    val blockConfig = ComputeLSTConfig(
      hConfFile = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      rasterFile = "/Users/feihu/Documents/GitHub/SparkCity/data/r-g-nir.tif",
      rasterCRS = "epsg:32618",
      longitudeFirst = true,
      vectorIndexTableName = "cb_2016_51_bg_500k",
      vectorCRS = "epsg:4269",
      osmLayerName = "block_a",
      outputShpPath = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_block_va/lst_block_va_lowAcc.shp"
    )

    val configs = Array(buildingsConfig, landuseConfig, poisConfig, trafficConfig, waterConfig, blockConfig)
    configs.foreach(config => {
      addLSTToOSMLayer(config)
      logInfo("Finished the processing of " + config.vectorIndexTableName)
    })
  }
}
