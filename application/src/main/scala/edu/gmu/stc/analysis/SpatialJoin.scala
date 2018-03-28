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
        }

        val row = geometry1.getUserData + "\t" + indices.mkString("\t")
        row.replace("\t", ",")
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

    val layerName = "landuse_a"

    val spatialJoinConfig = SpatialJoinConfig(
      confFilePath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml",
      envelope = new Envelope(-77.786, -76.914, 38.352, 39.151),
      gridType = "KDBTREE",
      indexType = "RTREE",
      partitionNum = 36,
      indexTable1 = "gis_osm_landuse_a_free_1",
      crs1 = "epsg:4326",
      longitudeFirst1 = true,
      indexTable2 = "gis_osm_buildings_a_free_1",
      crs2 = "epsg:4326",
      longitudeFirst2 = true,
      indexArray = Array("CP", "MPS", "MSI","MNND", "PCI", "FN", "RP"),
      rawAttributes = OSMAttributeUtil.getLayerAtrributes(layerName).asScala.toArray,
      outputFilePath = "data/test.csv"
    )

    spatialJoin(sc, spatialJoinConfig)
  }

}
