package edu.gmu.stc.vector.shapefile.meta;

import java.io.Serializable;

/**
 * Created by Fei Hu on 1/24/18.
 */
public class DbfMeta implements Serializable{
  private int index;
  private long offset;
  private int length;

  public DbfMeta(int index, long offset, int length) {
    this.index = index;
    this.offset = offset;
    this.length = length;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
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

  public String toString() {
    return String.format("index: %d, offset: %d, length: %d", index, offset, length);
  }
}
