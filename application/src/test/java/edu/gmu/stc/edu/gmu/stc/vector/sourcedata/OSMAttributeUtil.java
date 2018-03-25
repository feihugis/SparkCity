package edu.gmu.stc.edu.gmu.stc.vector.sourcedata;

/**
 * Created by Fei Hu on 3/24/18.
 */
public class OSMAttributeUtil {

  public static void testGetLayerAtrributes() {
    edu.gmu.stc.vector.sourcedata.OSMAttributeUtil.getLayerAtrributes("landuse_a");
    edu.gmu.stc.vector.sourcedata.OSMAttributeUtil.getLayerAtrributes("pois_a");
    edu.gmu.stc.vector.sourcedata.OSMAttributeUtil.getLayerAtrributes("buildings_a");
    edu.gmu.stc.vector.sourcedata.OSMAttributeUtil.getLayerAtrributes("traffic_a");
  }

  public static void main(String[] args) {
    testGetLayerAtrributes();
  }

}
