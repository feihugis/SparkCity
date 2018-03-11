package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.vector.examples.ShapeFileMetaTest._
import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest.logError
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{GridType, IndexType}

/**
  * Created by Fei Hu on 1/29/18.
  */
object STC_OverlapTest_V1 extends Logging{

  def overlap(args: Array[String], sc: SparkContext, spark: SparkSession): String = {
    if (args.length != 5) {
      logError("Please input four arguments: " +
        "\n \t 1)configFilePath: this file path for the configuration file path" +
        "\n \t 2) numPartition: the number of partitions" +
        "\n \t 3) gridType: the type of the partition, e.g. EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE" +
        "\n \t 4) indexType: the index type for each partition, e.g. QUADTREE, RTREE" +
        "\n \t 5) output file path: the file path for geojson output")

      return "Please input the right arguments"
    }

    sc.getConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    val configFilePath = args(0)   //"/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(configFilePath))
    sc.hadoopConfiguration.addResource(hConf)

    val tableNames = hConf.get(ConfigParameter.SHAPEFILE_INDEX_TABLES).split(",").map(s => s.toLowerCase().trim)

    val partitionNum = args(1).toInt  //24
    val minX = -180
    val minY = -180
    val maxX = 180
    val maxY = 180

    val gridType = GridType.getGridType(args(2)) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
    val indexType = IndexType.getIndexType(args(3))  //RTREE, QUADTREE

    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    val table1 = tableNames(0)
    shapeFileMetaRDD1.initializeShapeFileMetaRDD(sc, table1, gridType, partitionNum, minX, minY, maxX, maxY)
    shapeFileMetaRDD1.indexPartition(indexType)
    shapeFileMetaRDD1.getIndexedShapeFileMetaRDD.cache()

    val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
    val table2 = tableNames(1)
    shapeFileMetaRDD2.initializeShapeFileMetaRDD(sc, shapeFileMetaRDD1.getPartitioner, table2,
      partitionNum, minX, minY, maxX, maxY)

    shapeFileMetaRDD2.getShapeFileMetaRDD.cache()

    val geometryRDD = new GeometryRDD
    geometryRDD.intersect(shapeFileMetaRDD1, shapeFileMetaRDD2, partitionNum)
    geometryRDD.cache()

    val filePath = args(4)
    if (filePath.endsWith("shp")) {
      geometryRDD.saveAsShapefile(filePath)
    } else {
      geometryRDD.saveAsGeoJSON(filePath)
    }

    filePath
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      logError("Please input four arguments: " +
        "\n \t 1)configFilePath: this file path for the configuration file path" +
        "\n \t 2) numPartition: the number of partitions" +
        "\n \t 3) gridType: the type of the partition, e.g. EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE" +
        "\n \t 4) indexType: the index type for each partition, e.g. QUADTREE, RTREE" +
        "\n \t 5) output file path: the file path for geojson output")

      return
    }

    val sparkConf = new SparkConf().setAppName("%s_%s_%s_%s".format("STC_OverlapTest_v1", args(1), args(2), args(3)))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    val configFilePath = args(0)   //"/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(configFilePath))
    sc.hadoopConfiguration.addResource(hConf)

    val tableNames = hConf.get(ConfigParameter.SHAPEFILE_INDEX_TABLES).split(",").map(s => s.toLowerCase().trim)

    val partitionNum = args(1).toInt  //24
    val minX = -180
    val minY = -180
    val maxX = 180
    val maxY = 180

    val gridType = GridType.getGridType(args(2)) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
    val indexType = IndexType.getIndexType(args(3))  //RTREE, QUADTREE

    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    val table1 = tableNames(0)
    shapeFileMetaRDD1.initializeShapeFileMetaRDD(sc, table1, gridType, partitionNum, minX, minY, maxX, maxY)
    shapeFileMetaRDD1.indexPartition(indexType)
    shapeFileMetaRDD1.getIndexedShapeFileMetaRDD.cache()
    println("******shapeFileMetaRDD1****************", shapeFileMetaRDD1.getShapeFileMetaRDD.count())

    val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
    val table2 = tableNames(1)
    shapeFileMetaRDD2.initializeShapeFileMetaRDD(sc, shapeFileMetaRDD1.getPartitioner, table2,
      partitionNum, minX, minY, maxX, maxY)

    shapeFileMetaRDD2.getShapeFileMetaRDD.cache()

    println("******shapeFileMetaRDD2****************", shapeFileMetaRDD2.getShapeFileMetaRDD.count())

    println(shapeFileMetaRDD1.getShapeFileMetaRDD.partitions.size, "**********************",
      shapeFileMetaRDD2.getShapeFileMetaRDD.partitions.size)

    val geometryRDD = new GeometryRDD
    geometryRDD.intersect(shapeFileMetaRDD1, shapeFileMetaRDD2, partitionNum)
    geometryRDD.cache()

    val filePath = args(4)
    if (filePath.endsWith("shp")) {
      geometryRDD.saveAsShapefile(filePath)
    } else {
      geometryRDD.saveAsGeoJSON(filePath)
    }
    println("******** Number of intersected polygons: %d".format(geometryRDD.getGeometryRDD.count()))
  }

}
