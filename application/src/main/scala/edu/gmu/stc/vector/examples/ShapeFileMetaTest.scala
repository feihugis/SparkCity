package edu.gmu.stc.vector.examples

import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{GridType, IndexType}

/**
  * Created by Fei Hu on 1/24/18.
  */
object ShapeFileMetaTest extends App with Logging{

  val sparkConf = new SparkConf().setMaster("local[6]")
    .setAppName("ShapeFileMetaTest")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

  val confFilePath = "/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
  val sc = new SparkContext(sparkConf)
  val hConf = new Configuration()
  hConf.addResource(new Path(confFilePath))

  sc.hadoopConfiguration.addResource(hConf)

  //val tableName = "Impervious_Surface_2015_DC".toLowerCase
  val tableName = "Soil_Type_by_Slope_DC".toLowerCase
  hConf.set("mapred.input.dir", "/Users/feihu/Documents/GitHub/GeoSpark/application/src/main/resources/data/Washington_DC/" + tableName)

  //val shapeFileMetaRDD = new ShapeFileMetaRDD
  //shapeFileMetaRDD.initializeShapeFileMetaRDD(sc, hConf)
  //shapeFileMetaRDD.saveShapeFileMetaToDB(tableName)

  val minX = -180
  val minY = -180
  val maxX = 180
  val maxY = 180
  val partitionNum = 24
  val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
  val table1 = "Impervious_Surface_2015_DC".toLowerCase
  val gridType = GridType.getGridType("RTREE") //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
  shapeFileMetaRDD1.initializeShapeFileMetaRDD(sc, table1, gridType, partitionNum, minX, minY, maxX, maxY)
  shapeFileMetaRDD1.indexPartition(IndexType.RTREE)
  shapeFileMetaRDD1.getIndexedShapeFileMetaRDD.cache()
  println("******shapeFileMetaRDD1****************", shapeFileMetaRDD1.getShapeFileMetaRDD.count())

  val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
  val table2 = "Soil_Type_by_Slope_DC".toLowerCase
  shapeFileMetaRDD2.initializeShapeFileMetaRDD(sc, shapeFileMetaRDD1.getPartitioner, table2, partitionNum, minX, minY, maxX, maxY)
  shapeFileMetaRDD2.getShapeFileMetaRDD.cache()

  println("******shapeFileMetaRDD2****************", shapeFileMetaRDD2.getShapeFileMetaRDD.count())

  println(shapeFileMetaRDD1.getShapeFileMetaRDD.partitions.size, "**********************", shapeFileMetaRDD2.getShapeFileMetaRDD.partitions.size)

  /*val jointRDD = shapeFileMetaRDD1.spatialJoin(shapeFileMetaRDD2)

  println("**********************", jointRDD.partitions.size)*/


  /*shapeFileMetaRDD1.getShapeFileMetaRDD.mapPartitionsWithIndex((index, itor) => {
    itor.map(shapeFileMeta => (index, shapeFileMeta))
  }, true)*/

  /*val geometryRDD = new GeometryRDD()
  geometryRDD.initialize(shapeFileMetaRDD1, false)
  println("**********************", geometryRDD.getGeometryRDD.count())*/

  /*val geometryRDD = shapeFileMetaRDD1.spatialIntersect(shapeFileMetaRDD2)
  println("**********Intersected Polygons************", geometryRDD.count())*/

  val geometryRDD = new GeometryRDD
  geometryRDD.intersect(shapeFileMetaRDD1, shapeFileMetaRDD2, partitionNum)
  logInfo("******** Number of intersected polygons: %d".format(geometryRDD.getGeometryRDD.count()))
}
