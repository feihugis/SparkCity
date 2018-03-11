package edu.gmu.stc.vector.examples

import edu.gmu.stc.vector.operation.Overlap.intersect
import edu.gmu.stc.vector.operation.OperationUtil.show_timing
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

/**
  * Created by Fei Hu.
  */
object OverlapExample extends App {
  val LOG = Logger.getLogger("OverlapExample")

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  LOG.info("Start GeoSpark Overlap Spatial Analysis Example")

  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("GeoSpark-Analysis")
    .getOrCreate()

  //val sqlContext = new SQLContext(sc)
  //val sparkSession: SparkSession = sqlContext.sparkSession
  GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

  val resourceFolder = "/Users/feihu/Documents/GitHub/GeoSpark/application/src/main/resources/data/Washington_DC/"
  //val resourceFolder = "/user/root/data0119/"
  val shpFile1 = resourceFolder + "Impervious_Surface_2015_DC"
  val shpFile2 = resourceFolder + "Soil_Type_by_Slope_DC"
  val outputFile = resourceFolder + "dc_overlayMap_intersect_" + System.currentTimeMillis() + ".geojson"

  val numPartition = 24

  /*val plg1RDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, shpFile1)
  println(plg1RDD.rawSpatialRDD.count())*/

  val runtime = show_timing(intersect(sparkSession, "RTREE", "QUADTREE", shpFile1, shpFile2, numPartition, outputFile))
  LOG.info(s"*** Runtime is : $runtime")
  LOG.info("Finish GeoSpark Overlap Spatial Analysis Example")

  sparkSession.stop()
}
