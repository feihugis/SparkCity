package edu.gmu.stc.vector.transformation;

import com.vividsolutions.jts.geom.Polygon;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by Fei Hu on 1/19/18.
 */
public class Transformation {

  public static class HashSetToIterator implements FlatMapFunction<HashSet<Polygon>, Polygon> {

    @Override
    public Iterator<Polygon> call(HashSet<Polygon> polygons)
        throws Exception {
      return polygons.iterator();
    }
  }

}
