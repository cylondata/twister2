package edu.iu.dsc.tws.graphapi.vertex;

import java.util.ArrayList;

public class PageVertex {
  private String id;
  private  Double value;

  private ArrayList<String> arrayList = new ArrayList<>();

  public String getId() {
    return id;
  }

  public Double getValue() {
    return value;
  }

  public ArrayList<String> getArrayList() {
    return arrayList;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setValue(Double value) {
    this.value = value;
  }

  public void setArrayList(ArrayList<String> arrayList) {
    this.arrayList = arrayList;
  }
}
