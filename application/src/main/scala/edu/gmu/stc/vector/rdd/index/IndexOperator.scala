package edu.gmu.stc.vector.rdd.index

import com.vividsolutions.jts.geom.{Envelope, Geometry, GeometryFactory}
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.datasyslab.geospark.enums.IndexType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by Fei Hu on 1/27/18.
  */
class IndexOperator(indexType: String) extends Serializable {

  def buildIndex(iterator: Iterator[Geometry]): Iterator[SpatialIndex] = {
    val spatialIndex: SpatialIndex = if (IndexType.getIndexType(indexType) == IndexType.RTREE) {
                                          new STRtree
                                        } else {
                                          new Quadtree
                                        }

    while (iterator.hasNext) {
      val shapeFileMeta = iterator.next()
      spatialIndex.insert(shapeFileMeta.getEnvelopeInternal, shapeFileMeta)
    }

    spatialIndex.query(new Envelope(0.0, 0.0, 0.0, 0.0))

    List(spatialIndex).toIterator
  }


}

object IndexOperator extends Logging{

  def spatialJoin[T <: Geometry](iterator1: Iterator[SpatialIndex],
                  iterator2: Iterator[T])
  : Iterator[(T, T)] = {
    if (iterator1.isEmpty || iterator2.isEmpty) {
      return List[(T, T)]().iterator
    }

    iterator1.map(spatialIndex => {
      iterator2.flatMap(shapeMeta => {
        val overlapped = spatialIndex.query(shapeMeta.getEnvelopeInternal).asScala
          //.asInstanceOf[Iterator[ShapeFileMeta]]
        overlapped.map(shapeMeta1 => (shapeMeta1.asInstanceOf[T], shapeMeta))
      })
    }).foldLeft(Iterator[(T, T)]())(_ ++ _)
  }

  def geoSpatialIntersection[T <: Geometry](iterator1: Iterator[SpatialIndex],
                                            iterator2: Iterator[T])
  : Iterator[Geometry] = {
    if (iterator1.isEmpty || iterator2.isEmpty) {
      return List[Geometry]().iterator
    }

    iterator1.map(spatialIndex => {
      iterator2.flatMap(g2 => {
        val overlapped = spatialIndex.query(g2.getEnvelopeInternal).asScala
        //.asInstanceOf[Iterator[ShapeFileMeta]]
        overlapped.filter(g1 => g1.asInstanceOf[T].intersects(g2))
          .map(g1 => g1.asInstanceOf[T].intersection(g2))
      })
    }).foldLeft(Iterator[Geometry]())(_ ++ _)
  }

  def geoSpatialJoin[T <: Geometry](iterator1: Iterator[SpatialIndex],
                                            iterator2: Iterator[T])
  : Iterator[(T, T)] = {
    if (iterator1.isEmpty || iterator2.isEmpty) {
      return List[(T, T)]().iterator
    }

    iterator1.map(spatialIndex => {
      iterator2.flatMap(g2 => {
        val overlapped = spatialIndex.query(g2.getEnvelopeInternal).asScala
        overlapped.filter(g1 => g1.asInstanceOf[T].intersects(g2))
          .map(g1 => (g1.asInstanceOf[T], g2))
      })
    }).foldLeft(Iterator[(T, T)]())(_ ++ _)
  }



  def spatialJoinV2(iterator1: Iterator[SpatialIndex],
                  iterator2: Iterator[ShapeFileMeta])
  : Iterator[(Long, Long)] = {
    if (iterator1.isEmpty || iterator2.isEmpty) {
      return List[(Long, Long)]().iterator
    }

    iterator1.map(spatialIndex => {
      iterator2.flatMap(shapeMeta => {
        val overlapped = spatialIndex.query(shapeMeta.getEnvelopeInternal).asScala
        //.asInstanceOf[Iterator[ShapeFileMeta]]
        overlapped.map(shapeMeta1 => (shapeMeta1.asInstanceOf[ShapeFileMeta].getIndex.toLong, shapeMeta.getIndex.toLong))
      })
    }).foldLeft(Iterator[(Long, Long)]())(_ ++ _)
  }

  def spatialIntersect(iterator1: Iterator[SpatialIndex],
                  iterator2: Iterator[ShapeFileMeta])
  : Iterator[Geometry] = {
    val pairList = iterator1.map(spatialIndex => {
      iterator2.flatMap(shapeMeta => {
        val overlapped = spatialIndex.query(shapeMeta.getEnvelopeInternal).asScala
        overlapped.map(shapeMeta1 => (shapeMeta1.asInstanceOf[ShapeFileMeta], shapeMeta))
      })
    }).foldLeft(Iterator[(ShapeFileMeta, ShapeFileMeta)]())(_ ++ _).toList

    val shpPath1 = new Path(pairList.head._1.getFilePath + GeometryReaderUtil.SHP_SUFFIX)
    val shpPath2 = new Path(pairList.head._2.getFilePath + GeometryReaderUtil.SHP_SUFFIX)
    val fs = shpPath1.getFileSystem(new Configuration())
    val shpInputStream1 = fs.open(shpPath1)
    val shpInputStream2 = fs.open(shpPath2)
    val geometryFactory = new GeometryFactory()

    val hashMap1 = new HashMap[Long, Geometry]()
    val hashMap2 = new HashMap[Long, Geometry]()

    val time1 = System.currentTimeMillis()

    pairList.foreach(tuple => {
      if (!hashMap1.contains(tuple._1.getIndex)) {
        val geometry:Geometry = GeometryReaderUtil.readGeometry(shpInputStream1, tuple._1, geometryFactory)
        hashMap1 += (tuple._1.getIndex.toLong -> geometry)
      }

      if (!hashMap2.contains(tuple._2.getIndex)) {
        val geometry:Geometry = GeometryReaderUtil.readGeometry(shpInputStream2, tuple._2, geometryFactory)
        hashMap2 += (tuple._2.getIndex.toLong -> geometry)
      }
    })

    val time2 = System.currentTimeMillis()

    logInfo("********* Reading the number of geometries (%d, %d) took: %d seconds"
      .format(hashMap1.size, hashMap2.size, (time2 - time1)/1000))


    val result = pairList.map(tuple => {
      val geometry1 = hashMap1.getOrElse(tuple._1.getIndex.toLong, None)
      val geometry2 = hashMap2.getOrElse(tuple._2.getIndex.toLong, None)

      if (geometry1 != None && geometry2 != None) {
        geometry1.asInstanceOf[Geometry].intersection(geometry2.asInstanceOf[Geometry])
      } else {
        geometry1.asInstanceOf[Geometry]
      }
    }).toIterator

    val time3 = System.currentTimeMillis()

    logInfo("********* Intersecting %d pairs of geometries takes about : %d seconds"
      .format(pairList.size, (time3 - time2)/1000))

    result
  }

  def spatialIntersect(iterator: Iterator[(ShapeFileMeta, ShapeFileMeta)])
  : Iterator[Geometry] = {
    val pairList = iterator.toList

    val shpPath1 = new Path(pairList.head._1.getFilePath + GeometryReaderUtil.SHP_SUFFIX)
    val shpPath2 = new Path(pairList.head._2.getFilePath + GeometryReaderUtil.SHP_SUFFIX)
    val fs = shpPath1.getFileSystem(new Configuration())
    val shpInputStream1 = fs.open(shpPath1)
    val shpInputStream2 = fs.open(shpPath2)
    val geometryFactory = new GeometryFactory()

    val hashMap1 = new HashMap[Long, Geometry]()
    val hashMap2 = new HashMap[Long, Geometry]()

    val time1 = System.currentTimeMillis()

    pairList.foreach(tuple => {
      if (!hashMap1.contains(tuple._1.getIndex)) {
        val geometry:Geometry = GeometryReaderUtil.readGeometry(shpInputStream1, tuple._1, geometryFactory)
        hashMap1 += (tuple._1.getIndex.toLong -> geometry)
      }

      if (!hashMap2.contains(tuple._2.getIndex)) {
        val geometry:Geometry = GeometryReaderUtil.readGeometry(shpInputStream2, tuple._2, geometryFactory)
        hashMap2 += (tuple._2.getIndex.toLong -> geometry)
      }
    })

    val time2 = System.currentTimeMillis()

    logInfo("********* Reading the number of geometries (%d, %d) took: %d seconds"
      .format(hashMap1.size, hashMap2.size, (time2 - time1)/1000))


    val result = pairList.map(tuple => {
      val geometry1 = hashMap1.getOrElse(tuple._1.getIndex.toLong, None)
      val geometry2 = hashMap2.getOrElse(tuple._2.getIndex.toLong, None)

      geometry1.asInstanceOf[Geometry]

      val intersection = if (geometry1 != None && geometry2 != None && geometry1.asInstanceOf[Geometry].intersects(geometry2.asInstanceOf[Geometry])) {
        geometry1.asInstanceOf[Geometry].intersection(geometry2.asInstanceOf[Geometry])
      } else {
        None
      }
/*
      if (intersection.isEmpty && tuple._1.getEnvelopeInternal.intersects(tuple._2.getEnvelopeInternal)) {
        //logError("******** No overlapping " + tuple._1.getEnvelopeInternal.toString + ", " + tuple._2.getEnvelopeInternal.toString)
      }*/

      intersection
    }).filter(geometry => geometry != None).map(geometry => geometry.asInstanceOf[Geometry]).toIterator

    val time3 = System.currentTimeMillis()

    logInfo("********* Intersecting %d pairs of geometries takes about : %d seconds"
      .format(pairList.size, (time3 - time2)/1000))

    result
  }
}
