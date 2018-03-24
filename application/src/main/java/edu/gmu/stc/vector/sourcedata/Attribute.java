package edu.gmu.stc.vector.sourcedata;

/**
 * Created by Fei Hu on 3/24/18.
 */
public class Attribute {
  private int index;
  private String name;
  private Class Type;

  public Attribute(int index, String name, Class type) {
    this.name = name;
    this.index = index;
    Type = type;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Class getType() {
    return Type;
  }

  public void setType(Class type) {
    Type = type;
  }
}
