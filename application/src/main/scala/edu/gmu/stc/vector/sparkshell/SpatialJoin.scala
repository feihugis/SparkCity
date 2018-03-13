package edu.gmu.stc.vector.sparkshell

import com.vividsolutions.jts.geom.Envelope
import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.vector.operation.OperationUtil
import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v3.{logDebug, logError, logInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

/**
  * Created by Fei Hu on 3/11/18.
  */
object SpatialJoin extends Logging{

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      logError("You input "+ args.length + "arguments: " + args.mkString(" ") + ", but it requires 5 arguments: " +
        "\n \t 1)configFilePath: this file path for the configuration file path" +
        "\n \t 2) numPartition: the number of partitions" +
        "\n \t 3) gridType: the type of the partition, e.g. EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE" +
        "\n \t 4) indexType: the index type for each partition, e.g. QUADTREE, RTREE")

      return
    }

    val sparkConf = new SparkConf().setAppName("%s_%s_%s_%s".format("Spatial_Join", args(1), args(2), args(3)))
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

    // Bounding box for querying shapefiles
    val minX = -76.6517
    val minY = 39.2622
    val maxX = -76.5579
    val maxY = 39.3212

    //Transform query bbox projection
    val sourceCRS = CRS.decode("epsg:4326", true)
    val targetCRS = CRS.decode("epsg:32618", true)
    val transform = CRS.findMathTransform(sourceCRS, targetCRS, true)
    val bbox = JTS.transform(new Envelope(minX, minY, maxX, maxY), transform)


    val gridType = GridType.getGridType(args(2)) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
    val indexType = IndexType.getIndexType(args(3))  //RTREE, QUADTREE

    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    val table1 = tableNames(0)
    shapeFileMetaRDD1.initializeShapeFileMetaRDDAndPartitioner(sc, table1, gridType, partitionNum, minX, minY, maxX, maxY)
    val geometryRDD1 = new GeometryRDD
    geometryRDD1.initialize(shapeFileMetaRDD1, hasAttribute = false)
    geometryRDD1.partition(shapeFileMetaRDD1.getPartitioner)
    geometryRDD1.indexPartition(indexType)
    geometryRDD1.cache()

    val partitionNum1 = geometryRDD1.getGeometryRDD.mapPartitionsWithIndex({
      case (index, itor) => {
        List((index, itor.size)).toIterator
      }
    }).collect()

    logDebug("********geometryRDD1*************\n")
    OperationUtil.show_partitionInfo(geometryRDD1.getGeometryRDD)
    logDebug("******Geometry Num****************" + geometryRDD1.getGeometryRDD.count())
    logDebug("********geometryRDD1*************\n")

    val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
    val table2 = tableNames(1)



    shapeFileMetaRDD2.initializeShapeFileMetaRDDWithoutPartition(sc, table2,
      partitionNum, bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY)

    val geometryRDD2 = new GeometryRDD
    geometryRDD2.initialize(shapeFileMetaRDD2, hasAttribute = false)
    geometryRDD2.CRSTransfor("epsg:32618", "epsg:4326")
    geometryRDD2.partition(shapeFileMetaRDD1.getPartitioner)
    geometryRDD2.cache()

    logDebug("*************Counting GeometryRDD2 Time: " + OperationUtil.show_timing(geometryRDD2.getGeometryRDD.count()))


    logDebug("********geometryRDD2*************\n")
    OperationUtil.show_partitionInfo(geometryRDD2.getGeometryRDD)
    logDebug("******Geometry Num****************" + geometryRDD2.getGeometryRDD.count())
    logDebug("********geometryRDD2*************\n")


    val geometryRDD = geometryRDD1.spatialJoin(geometryRDD2)
    geometryRDD.cache()


    logDebug("******** Number of intersected polygons: %d".format(geometryRDD.count()))
  }

}
