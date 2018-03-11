package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.vector.operation.Overlap.intersect
import edu.gmu.stc.vector.operation.OperationUtil.show_timing
import edu.gmu.stc.vector.sparkshell.OverlapShellExample.sc
import edu.gmu.stc.vector.sparkshell.STC_OverlapTest_V1.logError
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

/**
  * Created by Fei Hu.
  */
object GeoSpark_OverlapTest extends Logging{

  def overlap(args: Array[String], sc: SparkContext, spark: SparkSession): String = {
    if (args.length != 6) {
      logError("Please input 6 arguments: " +
        "\n \t 1) input dir path: this directory path for the shapefiles" +
        "\n \t 2) shapefile layer 1 folder name: the name of the first shapefile" +
        "\n \t 3) shapefile layer 2 folder name: the name of the second shapefile" +
        "\n \t 4) number of partitions: the int number of partitions" +
        "\n \t 5) partition type: it can be EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE") +
        "\n \t 6) indexType: the index type for each partition, e.g. QUADTREE, RTREE"

      return "Please input the right arguments"
    }

    GeoSparkSQLRegistrator.registerAll(spark.sqlContext)

    val resourceFolder = args(0) //"/user/root/data0119/"
    val shpFile1: String = resourceFolder + "/" + args(1) //"Impervious_Surface_2015_DC"
    val shpFile2: String = resourceFolder + "/" + args(2) //"Soil_Type_by_Slope_DC"

    val numPartition: Int = args(3).toInt
    val outputFile: String = resourceFolder + "%s_%s_overlap_%s_".format(args(1), args(2), args(4)) + System.currentTimeMillis() + ".geojson"

    intersect(spark, args(4), args(5), shpFile1, shpFile2, numPartition, outputFile)
    outputFile
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      logError("Please input 6 arguments: " +
        "\n \t 1) input dir path: this directory path for the shapefiles" +
        "\n \t 2) shapefile layer 1 folder name: the name of the first shapefile" +
        "\n \t 3) shapefile layer 2 folder name: the name of the second shapefile" +
        "\n \t 4) number of partitions: the int number of partitions" +
        "\n \t 5) partition type: it can be EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE") +
        "\n \t 6) indexType: the index type for each partition, e.g. QUADTREE, RTREE"

      return
    }

    val sparkConf = new SparkConf().setAppName("GeoSpark_OverlapTest" + "_%s_%s_%s".format(args(3), args(4), args(5)))

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val sparkSession: SparkSession = sqlContext.sparkSession

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

    val resourceFolder = args(0) //"/user/root/data0119/"
    val shpFile1: String = resourceFolder + "/" + args(1) //"Impervious_Surface_2015_DC"
    val shpFile2: String = resourceFolder + "/" + args(2) //"Soil_Type_by_Slope_DC"

    val numPartition: Int = args(3).toInt
    val outputFile: String = resourceFolder + "%s_%s_overlap_%s".format(args(1), args(2), args(4)) + ".geojson"


    val runtime: Long = show_timing(intersect(sparkSession, args(4), args(5), shpFile1, shpFile2, numPartition, outputFile))
    println(s"Runtime is : $runtime seconds")

    sparkSession.stop()
  }
}
