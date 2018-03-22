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
import edu.gmu.vector.landscape.ComputeLandscape._
import org.apache.spark.sql.SQLContext

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
    val minX = -77.786
    val minY = 38.352
    val maxX = -76.914
    val maxY = 39.151

    /*val minX = -76.6517
    val minY = 39.2622
    val maxX = -76.5579
    val maxY = 39.3212*/

    //Transform query bbox projection
    /*val sourceCRS = CRS.decode("epsg:4326")
    val targetCRS = CRS.decode("epsg:32618")
    val transform = CRS.findMathTransform(sourceCRS, targetCRS)
    val envelope = new Envelope(minX, minY, maxX, maxY)
    val envelope2D = JTS.getEnvelope2D(envelope, sourceCRS)
    val bbox = JTS.transform(envelope, transform)*/


    val gridType = args(2) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
    val indexType = args(3)  //RTREE, QUADTREE
    val table1 = tableNames(0)
    val table2 = tableNames(1)
    val table3 = tableNames(2)

    val geometryRDD1 = GeometryRDD(sc, hConf, table1, gridType, indexType, partitionNum, minX, minY, maxX, maxY, true, true)

    logDebug("********geometryRDD1*************\n")
    OperationUtil.show_partitionInfo(geometryRDD1.getGeometryRDD)
    logInfo("******Geometry Num****************" + geometryRDD1.getGeometryRDD.count())

    val geometryRDD2 = GeometryRDD(sc, hConf, table2, partitionNum, geometryRDD1.getPartitioner, minX, minY, maxX, maxY, true, true)

    logDebug("*************Counting GeometryRDD2 Time: " + OperationUtil.show_timing(geometryRDD2.getGeometryRDD.count()))
    logDebug("********geometryRDD2*************\n")
    OperationUtil.show_partitionInfo(geometryRDD2.getGeometryRDD)
    logInfo("******Geometry Num****************" + geometryRDD2.getGeometryRDD.count())

    geometryRDD2.getGeometryRDD.foreach(geometry => println(geometry.getUserData))

    val joinedGeometryRDD12 = geometryRDD1.spatialJoin(geometryRDD2)
    joinedGeometryRDD12.cache()

    val landscapeMetricRDD = joinedGeometryRDD12.map {
      case (geoCover, geoFeatureList) => {
        (computeCoverPercent(geoCover, geoFeatureList),
        computeMeanNearestNeighborDistance(geoCover, geoFeatureList),
        computePatchCohesionIndex(geoCover, geoFeatureList))
      }
    }

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = landscapeMetricRDD.toDF()
    df.show(100)
    //joinedGeometryRDD12.foreach(tuple => println(tuple._2.foldLeft[Double](0.0)((area, g) => area + g.getArea)))

    /*val geometryRDD3 = GeometryRDD(sc, hConf, table3, partitionNum, geometryRDD1.getPartitioner, minX, minY, maxX, maxY, true, true)
    val joinedGeometryRDD13 = geometryRDD1.spatialJoin(geometryRDD3)

    joinedGeometryRDD13.foreach {
      case(geoBlock, geoItor) => {
        val (t, area) = geoItor.foldLeft[(Double, Double)]((0.0, 0.0))((tuple, geo) => {
          val area = geo.getArea
          val t = geo.getUserData.asInstanceOf[String].toDouble * area

          (tuple._1 + t, tuple._2 + area)
        })

        println(t/area + " : temperature")
      }
    }*/
  }

}
