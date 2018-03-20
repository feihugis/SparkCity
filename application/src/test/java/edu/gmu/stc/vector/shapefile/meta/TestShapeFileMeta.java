package edu.gmu.stc.vector.shapefile.meta;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class TestShapeFileMeta {

  static final Log LOG = LogFactory.getLog(TestShapeFileMeta.class);

  public static void testSQLForOverlappedRows() {
    System.out.println(ShapeFileMeta.getSQLForOverlappedRows("shapeFile",
                                                             -77.0411709654385,
                                                             38.9956105073279,
                                                             -77.0413824515586,
                                                             38.9954578167531));

  }

  public static void testEnvolopHashCode() {
    Envelope envelope1 = new Envelope();
    envelope1.init(1.0, 2.0, 1.0, 2.0);

    Envelope envelope2 = new Envelope();
    envelope2.init(1.0, 2.0, 1.0, 2.0);

    System.out.println(String.format("The hashcode for two same envelopes are : %d, %d",
                                     envelope1.hashCode(),
                                     envelope2.hashCode()));

    Envelope envelope3 = new Envelope();
    envelope1.init(-1.0, 2.0, 1.0, 2.0);

    Envelope envelope4 = new Envelope();
    envelope2.init(-1.0, 2.0, 1.0, 2.0);

    System.out.println(String.format("The hashcode for two same envelopes are : %d, %d",
                                     envelope3.hashCode(),
                                     envelope4.hashCode()));
  }

  public static void testSaveAsShapeFile() throws IOException, FactoryException {
    GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    Coordinate[] coords1  =
        new Coordinate[] {new Coordinate(4, 0), new Coordinate(2, 2),
                          new Coordinate(4, 4), new Coordinate(6, 2),
                          new Coordinate(4, 0) };

    LinearRing ring1 = geometryFactory.createLinearRing(coords1 );
    LinearRing holes1[] = null; // use LinearRing[] to represent holes
    Polygon polygon1 = geometryFactory.createPolygon(ring1, holes1 );

    Coordinate[] coords2  =
        new Coordinate[] {new Coordinate(14, 10), new Coordinate(12, 12),
                          new Coordinate(14, 14), new Coordinate(16, 12),
                          new Coordinate(14, 10) };

    LinearRing ring2 = geometryFactory.createLinearRing(coords1 );
    LinearRing holes2[] = null; // use LinearRing[] to represent holes
    Polygon polygon2 = geometryFactory.createPolygon(ring2, holes2 );

    List<Geometry> geometries = new ArrayList<Geometry>();
    geometries.add(polygon1);
    geometries.add(polygon2);

    GeometryReaderUtil.saveAsShapefile("test.shp", geometries, "epsg:4326");
  }

  public static void main(String[] args) throws IOException, FactoryException {
    testSQLForOverlappedRows();
    testEnvolopHashCode();
    testSaveAsShapeFile();
  }

}
