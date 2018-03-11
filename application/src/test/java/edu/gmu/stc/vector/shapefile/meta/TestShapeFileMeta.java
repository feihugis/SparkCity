package edu.gmu.stc.vector.shapefile.meta;

import com.vividsolutions.jts.geom.Envelope;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

  public static void main(String[] args) {
    testSQLForOverlappedRows();
    testEnvolopHashCode();
  }

}
