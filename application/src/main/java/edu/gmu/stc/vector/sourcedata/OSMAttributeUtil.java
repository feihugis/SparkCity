package edu.gmu.stc.vector.sourcedata;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fei Hu on 3/24/18.
 */

public class OSMAttributeUtil {
  public enum  OSMLayer {
    LANDUSE_AREA("landuse_a"),
    POIS_AREA("pois_a"),
    BUILDINGS_AREA("buildings_a"),
    TRAFFIC_AREA("traffic_a");

    private String layerName;

    OSMLayer(String osmLayerName) {
      this.layerName = osmLayerName;
    }

    public String layerName() {
      return this.layerName;
    }
  }

  public static List<Attribute> getOSMLanduseAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    return attributes;
  }

  public static List<Attribute> getOSMPOIsAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    return attributes;
  }

  public static List<Attribute> getOSMBuildingsAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    Attribute type = new Attribute(3, "type", String.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    attributes.add(type);
    return attributes;
  }

  public static List<Attribute> getOSMTrafficAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    return attributes;
  }

  public static List<Attribute> getLayerAtrributes(String layerName) {

    OSMLayer osmLayer = OSMLayer.valueOf(layerName.toLowerCase().trim());
    switch (osmLayer) {
      case LANDUSE_AREA:
        return getOSMLanduseAreaLayerAttribute();

      case POIS_AREA:
        return getOSMPOIsAreaLayerAttribute();

      case BUILDINGS_AREA:
        return getOSMBuildingsAreaLayerAttribute();

      case TRAFFIC_AREA:
        return getOSMTrafficAreaLayerAttribute();

      default:
        throw new AssertionError("Unknown OSMLayer: " + layerName);
    }
  }








}
