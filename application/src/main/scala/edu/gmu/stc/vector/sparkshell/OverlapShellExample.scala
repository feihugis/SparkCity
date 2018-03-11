package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.vector.operation.Overlap.intersect
import edu.gmu.stc.vector.operation.OperationUtil.show_timing
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

/**
  * Created by Fei Hu on 1/23/18.
  */
object OverlapShellExample {

  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  val sparkSession: SparkSession = sqlContext.sparkSession

  GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

  val resourceFolder = "/user/root/data0119/"
  val shpFile1: String = resourceFolder + "Impervious_Surface_2015_DC"
  val shpFile2: String = resourceFolder + "Soil_Type_by_Slope_DC"

  val outputFile: String = resourceFolder + "dc_overlayMap_intersect_" + System.currentTimeMillis() + ".geojson"
  val numPartition = 24
  val runtime: Long = show_timing(intersect(sparkSession, "KDBTREE", "QUADTREE", shpFile1, shpFile2, numPartition, outputFile))
}
