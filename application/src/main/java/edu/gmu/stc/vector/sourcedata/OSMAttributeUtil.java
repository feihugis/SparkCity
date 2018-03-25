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
    BLOCK_AREA("block_a");

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

  public static List<Attribute> getOSMLanduseAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    Attribute temperature = new Attribute(4, "temp", Double.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    attributes.add(temperature);
    return attributes;
  }

  public static List<Attribute> getOSMPOIsAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    Attribute temperature = new Attribute(4, "temp", Double.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    attributes.add(temperature);
    return attributes;
  }

  public static List<Attribute> getOSMBuildingsAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    Attribute type = new Attribute(4, "type", String.class);
    Attribute temperature = new Attribute(5, "temp", Double.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    attributes.add(type);
    attributes.add(temperature);
    return attributes;
  }

  public static List<Attribute> getOSMTrafficAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    Attribute temperature = new Attribute(4, "temp", Double.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    attributes.add(temperature);
    return attributes;
  }

  public static List<Attribute> getOSMWaaterAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute osm_id = new Attribute(0, "osm_id", Long.class);
    Attribute code = new Attribute(1, "code", Long.class);
    Attribute fclass = new Attribute(2, "fclass", String.class);
    Attribute name = new Attribute(3, "name", String.class);
    Attribute temperature = new Attribute(4, "temp", Double.class);
    attributes.add(osm_id);
    attributes.add(code);
    attributes.add(fclass);
    attributes.add(name);
    attributes.add(temperature);
    return attributes;
  }

  public static List<Attribute> getBlockAreaLayerAttribute() {
    List<Attribute> attributes = new ArrayList<Attribute>();
    Attribute STATEFP = new Attribute(0, "STATEFP", Integer.class);
    Attribute COUNTYFP = new Attribute(1, "COUNTYFP", String.class);
    Attribute TRACTCE = new Attribute(2, "TRACTCE", String.class);
    Attribute BLKGRPCE = new Attribute(3, "BLKGRPCE", Integer.class);
    Attribute AFFGEOID = new Attribute(4, "AFFGEOID", String.class);
    Attribute GEOID = new Attribute(5, "GEOID", String.class);
    Attribute NAME = new Attribute(6, "NAME", Integer.class);
    Attribute LSAD = new Attribute(7, "LSAD", String.class);
    Attribute ALAND = new Attribute(8, "ALAND", String.class);
    Attribute AWATER = new Attribute(9, "AWATER", String.class);
    Attribute temperature = new Attribute(10, "temp", Double.class);

    attributes.add(STATEFP);
    attributes.add(COUNTYFP);
    attributes.add(TRACTCE);
    attributes.add(BLKGRPCE);
    attributes.add(AFFGEOID);
    attributes.add(GEOID);
    attributes.add(NAME);
    attributes.add(LSAD);
    attributes.add(ALAND);
    attributes.add(AWATER);
    attributes.add(temperature);
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
        return getOSMWaaterAreaLayerAttribute();

      case BLOCK_AREA:
        return getBlockAreaLayerAttribute();

      default:
        throw new AssertionError("Unknown OSMLayer: " + layerName);
    }
  }
}
