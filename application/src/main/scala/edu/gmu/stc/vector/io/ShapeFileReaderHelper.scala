package edu.gmu.stc.vector.io

import com.vividsolutions.jts.geom.Geometry
import edu.gmu.stc.hibernate.{DAOImpl, HibernateUtil, PhysicalNameStrategyImpl}
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

/**
  * Created by Fei Hu on 3/21/18.
  */
object ShapeFileReaderHelper extends Logging{

  def queryShapeMetaDatas(hconf: Configuration,
           tableName: String,
           minX: Double, minY: Double,
           maxX: Double, maxY: Double,
           hasAttribute: Boolean): List[ShapeFileMeta] = {
    val physicalNameStrategy = new PhysicalNameStrategyImpl(tableName)
    val hibernateUtil = new HibernateUtil
    hibernateUtil.createSessionFactoryWithPhysicalNamingStrategy(hconf,
      physicalNameStrategy,
      classOf[ShapeFileMeta])
    val session = hibernateUtil.getSession
    val dao = new DAOImpl[ShapeFileMeta]()
    dao.setSession(session)
    val hql = ShapeFileMeta.getSQLForOverlappedRows(tableName, minX, minY, maxX, maxY)
    logInfo(hql)

    val shapeFileMetaList = dao.findByQuery(hql, classOf[ShapeFileMeta]).asScala.toList
    session.close()
    hibernateUtil.closeSessionFactory()
    shapeFileMetaList
  }

  def read(hconf: Configuration,
           tableName: String,
           minX: Double, minY: Double,
           maxX: Double, maxY: Double,
           hasAttribute: Boolean): List[Geometry] = {
    val shapeFileMetaList = queryShapeMetaDatas(hconf, tableName, minX, minY, maxX, maxY, hasAttribute)

    if (hasAttribute) {
      GeometryReaderUtil.readGeometriesWithAttributes(shapeFileMetaList.asJava).asScala.toList
    } else {
      GeometryReaderUtil.readGeometries(shapeFileMetaList.asJava).asScala.toList
    }
  }



}
