package edu.gmu.stc.analysis

import edu.gmu.stc.vector.rdd.ShapeFileMetaRDD
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest.logError
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Fei Hu on 3/22/18.
  */
object BuildShapeFileIndex extends Logging{

  def buildIndex(sc: SparkContext, hConf: Configuration): Unit = {
    sc.hadoopConfiguration.addResource(hConf)

    val shapeFileMetaRDD = new ShapeFileMetaRDD(sc, hConf)
    shapeFileMetaRDD.initializeShapeFileMetaRDD(sc, hConf)
    shapeFileMetaRDD.saveShapeFileMetaToDB()
  }

  def buildIndex_VA(sc: SparkContext): Unit = {
    val confPath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_build_index.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(confPath))
    hConf.set("mapred.input.dir", "/Users/feihu/Documents/GitHub/SparkCity/data/va")
    buildIndex(sc, hConf)
  }

  def buildIndex_DC(sc: SparkContext): Unit = {
    val confPath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_build_index.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(confPath))
    hConf.set("mapred.input.dir", "/Users/feihu/Documents/GitHub/SparkCity/data/dc")
    buildIndex(sc, hConf)
  }

  def buildIndex_MD(sc: SparkContext): Unit = {
    val confPath = "/Users/feihu/Documents/GitHub/SparkCity/config/conf_build_index.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(confPath))
    hConf.set("mapred.input.dir", "/Users/feihu/Documents/GitHub/SparkCity/data/md")
    buildIndex(sc, hConf)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("BuildShapefileIndex")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)
    //buildIndex_DC(sc)
    buildIndex_VA(sc)
    //buildIndex_MD(sc)
  }

}
