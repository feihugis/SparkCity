package edu.gmu.stc.vector.operation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator


/**
  * Created by Fei Hu
  */
object Overlap {

  val isDebug: Boolean = System.getProperty("spark.isDebug", "false").toBoolean

  val LOG: Logger = Logger.getLogger(Overlap.getClass)

  def intersect(sparkSession: SparkSession,
                gridType: String,
                indexType: String,
                shpFile1: String, shpFile2: String,
                numPartitions: Int,
                outputFile: String): Unit = {

    val plg1RDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, shpFile1)
    val plg2RDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, shpFile2)

    // get the boundary of RDD
    plg1RDD.analyze()
    plg2RDD.analyze()

    // partition RDD and buildIndex for each partition
    val PARTITION_TYPE = GridType.getGridType(gridType)
    val INDEX_TYPE = IndexType.getIndexType(indexType)

    plg1RDD.spatialPartitioning(PARTITION_TYPE, numPartitions)
    plg1RDD.buildIndex(INDEX_TYPE, true)
    plg1RDD.indexedRDD = plg1RDD.indexedRDD.cache()


    plg2RDD.spatialPartitioning(plg1RDD.getPartitioner)
    plg2RDD.buildIndex(INDEX_TYPE, true)
    plg2RDD.indexedRDD = plg2RDD.indexedRDD.cache()

    // overlap operation
    val intersectRDD = JoinQuery.SpatialIntersectQuery(plg1RDD, plg2RDD, true, true)
    val intersectedResult = new PolygonRDD(intersectRDD)


    if (isDebug) {
      val numOfPolygons = intersectedResult.countWithoutDuplicates()
      LOG.info(s"****** Number of polygons: $numOfPolygons")
    }

    intersectedResult.saveAsGeoJSON(outputFile)
  }
}
