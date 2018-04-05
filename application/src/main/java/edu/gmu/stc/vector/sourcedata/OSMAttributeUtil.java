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
    TRAFFIC_AREA("traffic_a"),
    WATER_AREA("water_a"),
    BLOCK_AREA("block_a"),
    ROADS("roads");

    private String layerName;

    OSMLayer(String osmLayerName) {
      this.layerName = osmLayerName;
    }

    public String layerName() {
      return this.layerName;
    }

    public static OSMLayer getByLayerName(String layerName) {
      layerName = layerName.toLowerCase().trim();
      for (OSMLayer osmLayer : OSMLayer.values()) {
        if (layerName.equals(osmLayer.layerName)) {
          return osmLayer;
        }
      }
      throw new AssertionError("No OSM Layer: " + layerName);
    }
  }

  public static String[] extraAttributes = new String[]{"lst", "ndvi", "ndwi", "ndbi", "ndii", "mndwi", "ndisi"};

  public static List<Attribute> getOSMLanduseAreaLayerAttribute() {
    int i = 0;
    List<Attribute> attributes = new ArrayList<Attribute>();
    attributes.add(new Attribute(i++, "osm_id", Long.class));
    attributes.add(new Attribute(i++, "code", Long.class));
    attributes.add(new Attribute(i++, "fclass", String.class));
    attributes.add(new Attribute(i++, "name", String.class));

    for (String extraAttribute : extraAttributes) {
      attributes.add(new Attribute(i++, extraAttribute, Double.class));
    }

    return attributes;
  }

  public static List<Attribute> getOSMPOIsAreaLayerAttribute() {
    int i = 0;
    List<Attribute> attributes = new ArrayList<Attribute>();

    attributes.add(new Attribute(i++, "osm_id", Long.class));
    attributes.add(new Attribute(i++, "code", Long.class));
    attributes.add(new Attribute(i++, "fclass", String.class));
    attributes.add(new Attribute(i++, "name", String.class));

    for (String extraAttribute : extraAttributes) {
      attributes.add(new Attribute(i++, extraAttribute, Double.class));
    }

    return attributes;
  }

  public static List<Attribute> getOSMBuildingsAreaLayerAttribute() {
    int i = 0;
    List<Attribute> attributes = new ArrayList<Attribute>();

    attributes.add(new Attribute(i++, "osm_id", Long.class));
    attributes.add(new Attribute(i++, "code", Long.class));
    attributes.add(new Attribute(i++, "fclass", String.class));
    attributes.add(new Attribute(i++, "name", String.class));
    attributes.add(new Attribute(i++, "type", String.class));

    for (String extraAttribute : extraAttributes) {
      attributes.add(new Attribute(i++, extraAttribute, Double.class));
    }

    return attributes;
  }

  public static List<Attribute> getOSMTrafficAreaLayerAttribute() {
    int i = 0;

    List<Attribute> attributes = new ArrayList<Attribute>();
    attributes.add(new Attribute(i++, "osm_id", Long.class));
    attributes.add(new Attribute(i++, "code", Long.class));
    attributes.add(new Attribute(i++, "fclass", String.class));
    attributes.add(new Attribute(i++, "name", String.class));

    for (String extraAttribute : extraAttributes) {
      attributes.add(new Attribute(i++, extraAttribute, Double.class));
    }

    return attributes;
  }

  public static List<Attribute> getOSMWaterAreaLayerAttribute() {
    int i = 0;

    List<Attribute> attributes = new ArrayList<Attribute>();
    attributes.add(new Attribute(i++, "osm_id", Long.class));
    attributes.add(new Attribute(i++, "code", Long.class));
    attributes.add(new Attribute(i++, "fclass", String.class));
    attributes.add(new Attribute(i++, "name", String.class));

    for (String extraAttribute : extraAttributes) {
      attributes.add(new Attribute(i++, extraAttribute, Double.class));
    }

    return attributes;
  }

  public static List<Attribute> getRoadsLayerAttribute() {
    int i = 0;

    List<Attribute> attributes = new ArrayList<Attribute>();
    attributes.add(new Attribute(i++, "osm_id", Long.class));
    attributes.add(new Attribute(i++, "code", Long.class));
    attributes.add(new Attribute(i++, "fclass", String.class));
    attributes.add(new Attribute(i++, "name", String.class));
    attributes.add(new Attribute(i++, "ref", String.class));
    attributes.add(new Attribute(i++, "oneway", String.class));
    attributes.add(new Attribute(i++, "maxspeed", Integer.class));
    attributes.add(new Attribute(i++, "layer", Integer.class));
    attributes.add(new Attribute(i++, "bridge", String.class));
    attributes.add(new Attribute(i++, "tunnel", String.class));

    for (String extraAttribute : extraAttributes) {
      attributes.add(new Attribute(i++, extraAttribute, Double.class));
    }

    return attributes;
  }

  public static List<Attribute> getBlockAreaLayerAttribute() {
    int i = 0;

    List<Attribute> attributes = new ArrayList<Attribute>();

    attributes.add(new Attribute(i++, "STATEFP", Integer.class));
    attributes.add(new Attribute(i++, "COUNTYFP", String.class));
    attributes.add(new Attribute(i++, "TRACTCE", String.class));
    attributes.add(new Attribute(i++, "BLKGRPCE", Integer.class));
    attributes.add(new Attribute(i++, "AFFGEOID", String.class));
    attributes.add(new Attribute(i++, "GEOID", String.class));
    attributes.add(new Attribute(i++, "NAME", Integer.class));
    attributes.add(new Attribute(i++, "LSAD", String.class));
    attributes.add(new Attribute(i++, "ALAND", String.class));
    attributes.add(new Attribute(i++, "AWATER", String.class));

    for (String extraAttribute : extraAttributes) {
      attributes.add(new Attribute(i++, extraAttribute, Double.class));
    }

    return attributes;
  }

  public static List<Attribute> getLayerAtrributes(String layerName) {
    OSMLayer osmLayer = OSMLayer.getByLayerName(layerName);
    switch (osmLayer) {
      case LANDUSE_AREA:
        return getOSMLanduseAreaLayerAttribute();

      case POIS_AREA:
        return getOSMPOIsAreaLayerAttribute();

      case BUILDINGS_AREA:
        return getOSMBuildingsAreaLayerAttribute();

      case TRAFFIC_AREA:
        return getOSMTrafficAreaLayerAttribute();

      case WATER_AREA:
        return getOSMWaterAreaLayerAttribute();

      case BLOCK_AREA:
        return getBlockAreaLayerAttribute();

      case ROADS:
        return getRoadsLayerAttribute();

      default:
        throw new AssertionError("Unknown OSMLayer: " + layerName);
    }
  }
}
