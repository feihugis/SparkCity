package edu.gmu.stc.analysis

import com.vividsolutions.jts.geom._
import edu.gmu.stc.hdfs.HdfsUtils
import edu.gmu.stc.raster.io.GeoTiffReaderHelper
import edu.gmu.stc.raster.landsat.Calculations
import edu.gmu.stc.raster.operation.RasterOperation
import edu.gmu.stc.vector.VectorUtil
import edu.gmu.stc.vector.io.ShapeFileReaderHelper
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import edu.gmu.stc.vector.sourcedata.OSMAttributeUtil
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{DoubleConstantNoDataCellType, Tile}
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.geotools.geometry.jts.JTS

import scala.collection.JavaConverters._

/**
  * Created by Fei Hu on 3/21/18.
  */
object ComputeSpectralIndex extends Logging{

  case class ComputeSpectralIndexConfig(hConfFile: String,
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
  def addSpectralIndexToOSMLayer(computeSpectralIndexConfig: ComputeSpectralIndexConfig,
                                 spectralIndexNames: Array[String]): Unit = {
    val polygonsWithSpectralIndex = RasterOperation.extractMeanValueFromTileByGeometry(
      computeSpectralIndexConfig.hConfFile,
      computeSpectralIndexConfig.rasterFile,
      computeSpectralIndexConfig.rasterCRS,
      computeSpectralIndexConfig.rasterLongitudeFirst,
      computeSpectralIndexConfig.vectorIndexTableName,
      computeSpectralIndexConfig.vectorCRS,
      computeSpectralIndexConfig.vectorLongitudeFirst,
      spectralIndexNames)

    val attributeSchema = OSMAttributeUtil.getLayerAtrributes(computeSpectralIndexConfig.osmLayerName)

    GeometryReaderUtil.saveAsShapefile(
      computeSpectralIndexConfig.outputShpPath,
      computeSpectralIndexConfig.vectorCRS,
      classOf[Polygon],
      polygonsWithSpectralIndex.asJava,
      attributeSchema)

    GeometryReaderUtil.saveDbfAsCSV(
      polygonsWithSpectralIndex.asJava,
      attributeSchema,
      computeSpectralIndexConfig.outputShpPath.replace(".shp", ".csv"))
  }

  def addSpectralIndexToOSMLayer(landsatTiffPath: String,
                                 hConf: Configuration,
                                 computeSpectralIndexConfigs: Array[ComputeSpectralIndexConfig],
                                 spectralIndexNames: Array[String]): Unit = {
    val (tiffExtent, multiBandTiffTiles) = RasterOperation.readMultiBandGeoTiff(hConf, new Path(landsatTiffPath))
    val indexTiles = RasterOperation.computeSpectralIndex(multiBandTiffTiles, spectralIndexNames)

    computeSpectralIndexConfigs.foreach(computeSpectralIndexConfig => {
      val polygonsWithSpectralIndex = RasterOperation
        .extractMeanValueFromTileByGeometry(
          hConf,
          tiffExtent,
          indexTiles,
          computeSpectralIndexConfig.rasterCRS,
          computeSpectralIndexConfig.rasterLongitudeFirst,
          computeSpectralIndexConfig.vectorIndexTableName,
          computeSpectralIndexConfig.vectorCRS,
          computeSpectralIndexConfig.vectorLongitudeFirst
        )

      val attributeSchema = OSMAttributeUtil.getLayerAtrributes(computeSpectralIndexConfig.osmLayerName)

      GeometryReaderUtil.saveAsShapefile(
        computeSpectralIndexConfig.outputShpPath,
        computeSpectralIndexConfig.vectorCRS,
        classOf[Polygon],
        polygonsWithSpectralIndex.asJava,
        attributeSchema)

      GeometryReaderUtil.saveDbfAsCSV(
        polygonsWithSpectralIndex.asJava,
        attributeSchema,
        computeSpectralIndexConfig.outputShpPath.replace(".shp", ".csv"))
    })
  }

  def computeSpectralIndex(stateName: String, stateID: String,
                           landsatTiff: String, outputDir: String,
                           time: String,
                           hConfFile: String): Unit = {
    //val hConfFile = "config/conf_lst_va.xml"

    val buildingsConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_buildings_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "buildings_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_buildings_${time}.shp"
    )

    val landuseConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_landuse_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "landuse_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_landuse_${time}.shp"
    )

    val poisConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_pois_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "pois_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_pois_${time}.shp"
    )

    val trafficConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_traffic_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "traffic_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_traffic_${time}.shp"
    )

    val waterConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_water_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "water_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_water_${time}.shp"
    )

    val blockConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_cb_2016_${stateID}_bg_500k",
      vectorCRS = "epsg:4269",
      vectorLongitudeFirst = true,
      osmLayerName = "block_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_block_${time}.shp"
    )

    val spectralIndexNames = Array("lst", "ndvi", "ndwi", "ndbi", "ndii", "mndwi", "ndisi")
    val configs = Array(landuseConfig, poisConfig, trafficConfig, waterConfig, blockConfig/*, buildingsConfig*/)

    configs.foreach(config => {
      addSpectralIndexToOSMLayer(config, spectralIndexNames)
      logInfo("Finished the processing of " + config.vectorIndexTableName)
    })
  }

  def getSpectralIndexConfig(stateName: String, stateID: String,
                            landsatTiff: String, outputDir: String,
                            time: String,
                            hConfFile: String): Array[ComputeSpectralIndexConfig] = {
    //val hConfFile = "config/conf_lst_va.xml"

    val buildingsConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_buildings_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "buildings_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_buildings_${time}.shp"
    )

    val landuseConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_landuse_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "landuse_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_landuse_${time}.shp"
    )

    val poisConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_pois_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "pois_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_pois_${time}.shp"
    )

    val trafficConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_traffic_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "traffic_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_traffic_${time}.shp"
    )

    val waterConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_osm_water_a_free_1",
      vectorCRS = "epsg:4326",
      vectorLongitudeFirst = true,
      osmLayerName = "water_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_water_${time}.shp"
    )

    val blockConfig = ComputeSpectralIndexConfig(
      hConfFile = hConfFile,
      rasterFile = landsatTiff,
      rasterCRS = "epsg:32618",
      rasterLongitudeFirst = true,
      vectorIndexTableName = s"${stateName}_cb_2016_${stateID}_bg_500k",
      vectorCRS = "epsg:4269",
      vectorLongitudeFirst = true,
      osmLayerName = "block_a",
      outputShpPath = s"${outputDir}/${stateName}/lst/${stateName}_lst_block_${time}.shp"
    )

    val configs = Array(landuseConfig, poisConfig, trafficConfig, waterConfig, blockConfig/*, buildingsConfig*/)
    configs
  }

  def getSpectralIndexConfigCases(landsatTiff: String, time: String, outputDir: String, hConfFile: String): Array[ComputeSpectralIndexConfig] = {
    val configs1 = getSpectralIndexConfig("va", "51", landsatTiff, outputDir, time, hConfFile)
    val configs2 = getSpectralIndexConfig("md", "24", landsatTiff, outputDir, time, hConfFile)
    val configs3 = getSpectralIndexConfig("dc", "11", landsatTiff, outputDir, time, hConfFile)

    val configs = configs1 ++ configs2 ++ configs3
    configs
    /*val fs = FileSystem.get(sc.hadoopConfiguration)
    val hosts = HdfsUtils.getDataLocation(fs, new Path(landsatTiff))
    configs.map(config => (config, hosts))
*/

    /*sc.parallelize(configs).foreach(config => {
      addSpectralIndexToOSMLayer(config, spectralIndexNames)
      logInfo("Finished the processing of " + config.vectorIndexTableName)
    })*/
  }

  def computeSpectralIndexInParallel(sc: SparkContext,
                                     hConfFile: String,
                                     landsatTiffHDFSTxt: String, outputDir: String): Unit = {
    val spectralIndexNames = Array("lst", "ndvi", "ndwi", "ndbi", "ndii", "mndwi", "ndisi")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val landsatFilesWithDataLocation = sc.textFile(landsatTiffHDFSTxt).collect()
      .map(landsatFilePath => {
        val hosts = HdfsUtils.getDataLocation(fs, new Path(landsatFilePath))
        (landsatFilePath, hosts)
      })

    sc.makeRDD(landsatFilesWithDataLocation).foreach({
      case (landsatFilePath, hosts) => {
        val time = landsatFilePath.split("_")(3)
        val configs = getSpectralIndexConfigCases(landsatFilePath, time, outputDir, hConfFile)
        val hConf = new Configuration()
        HdfsUtils.addConfigXmlFromHDFS(hConf, hConfFile)
        addSpectralIndexToOSMLayer(landsatFilePath, hConf, configs, spectralIndexNames)
        logInfo("Finished the processing of " + landsatFilePath)
      }
    })



    /*val configCases = landsatFiles.flatMap(landsatFilePath => {
      val time = landsatFilePath.split("_")(3)
      getSpectralIndexConfigCases(sc, landsatFilePath, time, outputDir, hConfFile)
    })

    sc.makeRDD(configCases).foreach(config => {
      addSpectralIndexToOSMLayer(config._1, spectralIndexNames)
      logInfo("Finished the processing of " + config._1.vectorIndexTableName)
    })*/
  }


  def main(args: Array[String]): Unit = {

    // va 51
    // md 24
    // dc 11

    val landsatTiff = "data/landsat8_dc/LC08_L1TP_015033_20170416_20170501_01_T1/r-g-nir-tirs1-swir1.tif"
    val outputDir = "/Users/feihu/Documents/GitHub/SparkCity/data/20170416/"
    val hConfFile: String = "config/conf_lst_va.xml"
    val time = "20170416"


    val inputParameters = Array(
      ("va", "51", landsatTiff, outputDir),
      ("md", "24", landsatTiff, outputDir),
      ("dc", "11", landsatTiff, outputDir)
    )

    inputParameters.foreach({
      case (stateName: String, stateID: String, landsatTiff: String, outputDir: String) => {
        computeSpectralIndex(stateName, stateID, landsatTiff, outputDir, time, hConfFile)
      }
    })
  }
}
