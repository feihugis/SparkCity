package edu.gmu.stc.analysis

import java.io.{File, PrintWriter}

import com.vividsolutions.jts.geom.Envelope
import edu.gmu.stc.vector.VectorUtil
import edu.gmu.stc.vector.rdd.GeometryRDD
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import edu.gmu.stc.vector.sourcedata.{Attribute, OSMAttributeUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import edu.gmu.vector.landscape.ComputeLandscape._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.opengis.metadata.extent.Extent
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import scala.collection.JavaConverters._

/**
  * Created by Fei Hu on 3/22/18.
  */
object SpatialJoin {

  case class SpatialJoinConfig(confFilePath: String,
                               envelope: Envelope,
                               gridType: String,
                               indexType: String,
                               partitionNum: Int,
                               indexTable1: String,
                               crs1: String,
                               longitudeFirst1: Boolean,
                               indexTable2: String,
                               crs2: String,
                               longitudeFirst2: Boolean,
                               indexArray: Array[String],
                               rawAttributes: Array[Attribute],
                               outputFilePath: String)

  def spatialJoin(sc: SparkContext, spatialJoinConfig: SpatialJoinConfig): Unit = {
    spatialJoin(
      sc,
      spatialJoinConfig.confFilePath,
      spatialJoinConfig.envelope,
      spatialJoinConfig.gridType,
      spatialJoinConfig.indexType,
      spatialJoinConfig.partitionNum,
      spatialJoinConfig.indexTable1,
      spatialJoinConfig.crs1,
      spatialJoinConfig.longitudeFirst1,
      spatialJoinConfig.indexTable2,
      spatialJoinConfig.crs2,
      spatialJoinConfig.longitudeFirst2,
      spatialJoinConfig.indexArray,
      spatialJoinConfig.rawAttributes,
      spatialJoinConfig.outputFilePath
    )
  }

  def spatialJoin(sc: SparkContext, confFilePath: String,
                  envelope: Envelope,
                  gridType: String = "KDBTREE",
                  indexType: String = "RTREE",
                  partitionNum: Int,
                  indexTable1: String,
                  crs1: String,
                  longitudeFirst1: Boolean = true,
                  indexTable2: String,
                  crs2: String,
                  longitudeFirst2: Boolean = true,
                  indexArray: Array[String],
                  rawAttributes: Array[Attribute],
                  outputFilePath: String): Unit = {

    val hConf = new Configuration()
    hConf.addResource(new Path(confFilePath))
    sc.hadoopConfiguration.addResource(hConf)

    val minX = envelope.getMinX
    val minY = envelope.getMinY
    val maxX = envelope.getMaxX
    val maxY = envelope.getMaxY

    val baseLayerRDD = GeometryRDD(
      sc, hConf, indexTable1,
      gridType, indexType, partitionNum,
      minX, minY, maxX, maxY,
      readAttributes = true, isCache = true)

    val overlayedEnvelope = VectorUtil.transformCRS(
      envelope,
      sourceCRSStr = crs1,
      sourceLongitudeFirst = longitudeFirst1,
      targetCRSStr = crs2,
      targetLongitudeFirst = longitudeFirst2
    )

    val overlayedRDD = GeometryRDD(
      sc, hConf, indexTable2,
      partitionNum,
      baseLayerRDD.getPartitioner,
      overlayedEnvelope.getMinX,
      overlayedEnvelope.getMinY,
      overlayedEnvelope.getMaxX,
      overlayedEnvelope.getMaxY,
      readAttributes = true,
      isCache = true)

    val num = overlayedRDD.getGeometryRDD.count()

    if (!crs1.equalsIgnoreCase(crs2)) {
      overlayedRDD.transforCRS(crs2, longitudeFirst2, crs1, longitudeFirst1)
    }

    val joinedRDD = baseLayerRDD.spatialJoin(overlayedRDD).cache()

    val featureRDD = joinedRDD.map {
      case (geometry1, geometries2) => {
        val indices = indexArray.map {
          case "CP" => computeCoverPercent(geometry1, geometries2)
          case "FN" => countFeatureNum(geometry1, geometries2)
          case "MPS" => computeMeanPatchSize(geometry1, geometries2)
          case "MSI" => computeMeanShapeIndex(geometry1, geometries2)
          case "MNND" => computeMeanNearestNeighborDistance(geometry1, geometries2)
          case "PCI" => computePatchCohesionIndex(geometry1, geometries2)
          case "RP" => computeRoadPercent(geometry1, geometries2)
          case "TP" => computeCoverPercent(geometry1, geometries2)
        }

        geometry1
          .getUserData
          .toString
          .split("\t")
          .map(str => str.replace(",", " "))
          .++(indices).mkString(",")
      }
    }

    val result = featureRDD.collect()
    val col_names = rawAttributes
          .slice(0, result.head.split(",").length - indexArray.length)
          .map(attribute => attribute.getName).++(indexArray)

    GeometryReaderUtil.saveRows2CSV(col_names.mkString(","), result.toList.asJava, outputFilePath)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SpatialJoin")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    val va_bbox = (-77.622, -76.995, 38.658, 39.105)
    val va_statename = "va"
    val va_stateID = 51

    val dc_bbox = (-77.1512, -76.8916, 38.7882, 38.9980)
    val dc_statename = "dc"
    val dc_stateID = 11

    val md_bbox = (-76.7119, -76.5121, 39.2327, 39.3669)
    val md_statename = "md"
    val md_stateID = 24

    val stateName = va_statename
    val stateID = va_stateID
    val baseIndexTable = s"${stateName}_lst_block"
    val crs1 = "epsg:4269"
    val crs2 = "epsg:4326"

    val (minX, maxX, minY, maxY) = va_bbox
    val bbox = new Envelope(minX, maxX, minY, maxY)

    val spatialJoinConfig_0 = SpatialJoinConfig(
      confFilePath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      envelope = bbox,
      gridType = "KDBTREE",
      indexType = "RTREE",
      partitionNum = 36,
      indexTable1 = baseIndexTable,
      crs1 = crs1,
      longitudeFirst1 = true,
      indexTable2 = baseIndexTable,
      crs2 = crs2,
      longitudeFirst2 = true,
      indexArray = Array(),
      rawAttributes = OSMAttributeUtil.getLayerAtrributes("block_a").asScala.toArray,
      outputFilePath = s"data/${stateName}/result/${stateName}_cb.csv"
    )

    val spatialJoinConfig_1 = SpatialJoinConfig(
      confFilePath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      envelope = bbox,
      gridType = "KDBTREE",
      indexType = "RTREE",
      partitionNum = 36,
      indexTable1 = baseIndexTable,
      crs1 = crs1,
      longitudeFirst1 = true,
      indexTable2 = s"${stateName}_lst_buildings",
      crs2 = crs2,
      longitudeFirst2 = true,
      indexArray = Array("CP", "MPS", "MSI","MNND", "PCI", "FN"),
      rawAttributes = OSMAttributeUtil.getLayerAtrributes("block_a").asScala.toArray,
      outputFilePath = s"data/${stateName}/result/${stateName}_buildings.csv"
    )

    val spatialJoinConfig_2 = SpatialJoinConfig(
      confFilePath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      envelope = bbox,
      gridType = "KDBTREE",
      indexType = "RTREE",
      partitionNum = 36,
      indexTable1 = baseIndexTable,
      crs1 = crs1,
      longitudeFirst1 = true,
      indexTable2 = s"${stateName}_osm_roads_free_1",
      crs2 = crs2,
      longitudeFirst2 = true,
      indexArray = Array("RP"),
      rawAttributes = OSMAttributeUtil.getLayerAtrributes("block_a").asScala.toArray,
      outputFilePath = s"data/${stateName}/result/${stateName}_roads.csv"
    )

    val spatialJoinConfig_3 = SpatialJoinConfig(
      confFilePath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      envelope = bbox,
      gridType = "KDBTREE",
      indexType = "RTREE",
      partitionNum = 36,
      indexTable1 = baseIndexTable,
      crs1 = crs1,
      longitudeFirst1 = true,
      indexTable2 = s"${stateName}_lst_traffic",
      crs2 = crs2,
      longitudeFirst2 = true,
      indexArray = Array("TP"),
      rawAttributes = OSMAttributeUtil.getLayerAtrributes("block_a").asScala.toArray,
      outputFilePath = s"data/${stateName}/result/${stateName}_parkings.csv"
    )

    val spatialJoinConfig_4 = SpatialJoinConfig(
      confFilePath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      envelope = bbox,
      gridType = "KDBTREE",
      indexType = "RTREE",
      partitionNum = 36,
      indexTable1 = baseIndexTable,
      crs1 = crs1,
      longitudeFirst1 = true,
      indexTable2 = s"${stateName}_lst_water",
      crs2 = crs2,
      longitudeFirst2 = true,
      indexArray = Array("CP"),
      rawAttributes = OSMAttributeUtil.getLayerAtrributes("block_a").asScala.toArray,
      outputFilePath = s"data/${stateName}/result/${stateName}_water.csv"
    )

    spatialJoin(sc, spatialJoinConfig_0)
    spatialJoin(sc, spatialJoinConfig_1)
    spatialJoin(sc, spatialJoinConfig_2)
    spatialJoin(sc, spatialJoinConfig_3)
    spatialJoin(sc, spatialJoinConfig_4)
  }

}
