package edu.gmu.stc.vector.shapefile.meta;

import org.datasyslab.geospark.formatMapper.shapefileParser.boundary.BoundBox;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType;

/**
 * Created by Fei Hu on 1/24/18.
 */
public class ShpMeta {
  private Long index;
  private int typeID;
  private long offset;
  private int length;
  private BoundBox boundBox;

  public ShpMeta(Long index, int typeID, long offset, int length, BoundBox boundBox) {
    this.index = index;
    this.typeID = typeID;
    this.offset = offset;
    this.length = length;
    this.boundBox = boundBox;
  }

  public ShpMeta(int typeID, long offset, int length, BoundBox boundBox) {
    this.typeID = typeID;
    this.offset = offset;
    this.length = length;
    this.boundBox = boundBox;
  }

  public Long getIndex() {
    return index;
  }

  public void setIndex(Long index) {
    this.index = index;
  }

  public int getTypeID() {
    return typeID;
  }

  public void setTypeID(int typeID) {
    this.typeID = typeID;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public BoundBox getBoundBox() {
    return boundBox;
  }

  public void setBoundBox(BoundBox boundBox) {
    this.boundBox = boundBox;
  }

  public String toString() {
    return String.format("index: %d, typeID: %d, offset: %d, length: %d, Bbox: %s",
                         this.index, this.typeID, this.offset,
                         this.length, this.boundBox.toString());
  }
}
