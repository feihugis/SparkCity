package edu.gmu.stc.analysis

import edu.gmu.stc.vector.rdd.GeometryRDD
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import edu.gmu.vector.landscape.ComputeLandscape._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SQLContext
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression

/**
  * Created by Fei Hu on 3/22/18.
  */
object SpatialJoin {

  def spatialJoin() = {
    val sparkConf = new SparkConf().setAppName("SpatialJoin")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    val confFilePath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(confFilePath))
    sc.hadoopConfiguration.addResource(hConf)

    // Bounding box for querying shapefiles
    val minX = -77.786
    val minY = 38.352
    val maxX = -76.914
    val maxY = 39.151

    val lstTable = "lst_polygon_va"
    val gridType = "KDBTREE"
    val indexType = "RTREE"
    val partitionNum = 36
    val lstRDD = GeometryRDD(sc, hConf, lstTable,
      gridType, indexType, partitionNum,
      minX, minY, maxX, maxY, readAttributes = true, isCache = true)

    val bldTable = "gis_osm_buildings_a_free_1"
    val bldRDD = GeometryRDD(sc, hConf, bldTable,
      partitionNum,
      lstRDD.getPartitioner,
      minX, minY, maxX, maxY, readAttributes = true, isCache = true)

    val joinedRDD = lstRDD.spatialJoin(bldRDD).cache()

    val featureRDD = joinedRDD.map {
      case (lst, features) => {
        val CP = computeCoverPercent(lst, features)
        val MNND = computeMeanNearestNeighborDistance(lst, features)
        val PCI = computePatchCohesionIndex(lst, features)
        val attrs = lst.getUserData.toString.split("\t")
        val temperature = attrs(attrs.length - 1).toDouble

        temperature match {
          case t: Double => (lst.hashCode(), t, CP, MNND, PCI)
          case _ => (lst.hashCode(), -1.0, CP, MNND, PCI)
        }
      }
    }.filter(p => p._2 > 0)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    featureRDD.toDF("id", "temperature", "CP", "MNND", "PCI").coalesce(1).write.csv("data/lst_va.csv")

    val training = featureRDD.map(
      row =>
        LabeledPoint(row._2, Vectors.dense(row._3, row._4, row._5)))
      .toDF("label", "features")

    training.coalesce(1).write.format("libsvm").save("data/lst_va_libsvm")

    training.show(300)
  }



  def main(args: Array[String]): Unit = {
    spatialJoin()
  }

}
