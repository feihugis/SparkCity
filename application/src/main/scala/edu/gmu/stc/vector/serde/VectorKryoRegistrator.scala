package edu.gmu.stc.vector.serde

import com.esotericsoftware.kryo.Kryo
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.geometryObjects.{Circle, GeometrySerde, SpatialIndexSerde}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text

/**
  * Created by Fei Hu on 1/26/18.
  */
class VectorKryoRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    val serializer = new GeometrySerde
    val indexSerializer = new SpatialIndexSerde(serializer)

    kryo.register(classOf[ShapeFileMeta])

    kryo.register(classOf[Point], serializer)
    kryo.register(classOf[LineString], serializer)
    kryo.register(classOf[Polygon], serializer)
    kryo.register(classOf[MultiPoint], serializer)
    kryo.register(classOf[MultiLineString], serializer)
    kryo.register(classOf[MultiPolygon], serializer)
    kryo.register(classOf[GeometryCollection], serializer)
    kryo.register(classOf[Circle], serializer)
    kryo.register(classOf[Envelope], serializer)
    // TODO: Replace the default serializer with default spatial index serializer
    kryo.register(classOf[Quadtree], indexSerializer)
    kryo.register(classOf[STRtree], indexSerializer)
    kryo.register(classOf[SpatialIndex], indexSerializer)
    kryo.register(classOf[Tuple2[Polygon, Polygon]])

    //kryo.register(classOf[Configuration], new HadoopConfigurationSerde)
  }
}
