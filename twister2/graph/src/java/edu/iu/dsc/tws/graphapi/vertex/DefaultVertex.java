package edu.iu.dsc.tws.graphapi.vertex;


import java.util.ArrayList;

public class DefaultVertex<I, V, E> implements Vertex<String, Integer, DefaultEdge> {
  private String vertexId;
  private Integer vertexValue;
  private ArrayList<DefaultEdge> arrayList;

  @Override
  public void initialize(String id, Integer value, Iterable<DefaultEdge> edges) {
    this.vertexId =  id;
    this.vertexValue = value;
    this.arrayList = (ArrayList<DefaultEdge>) edges;
  }

  @Override
  public void initialize(String id, Integer value) {
    this.vertexId = id;
    this.vertexValue = value;
  }

  @Override
  public String getId() {
    return vertexId;
  }

  @Override
  public Integer getValue() {
    return vertexValue;
  }

  @Override
  public void setValue(Integer value) {
    this.vertexValue = value;
  }

  @Override
  public int getNumEdges() {
    return arrayList.size();
  }

  @Override
  public Iterable<DefaultEdge> getEdges() {
    return arrayList;
  }

  @Override
  public void setEdges(Iterable<DefaultEdge> edges) {
    this.arrayList = (ArrayList<DefaultEdge>) edges;
  }

  @Override
  public Integer getEdgeValue(String targetVertexId) {
    for (DefaultEdge defaultEdge : arrayList) {
      if (defaultEdge.getTargetVertexId().equals(targetVertexId)) {
        return defaultEdge.getValue();
      }
    }
    return null;
  }


  @Override
  public void setEdgeValue(String targetVertexId, Integer edgeValue) {
    for (DefaultEdge defaultEdge : arrayList) {
      if (defaultEdge.getTargetVertexId().equals(targetVertexId)) {
        defaultEdge.setValue(edgeValue);
      }
    }
  }

  @Override
  public void addEdge(DefaultEdge edge) {
    arrayList.add(edge);
  }

  @Override
  public void removeEdges(String targetVertexId) {
    for (DefaultEdge defaultEdge : arrayList) {
      if (defaultEdge.getTargetVertexId().equals(targetVertexId)) {
        arrayList.remove(defaultEdge);
      }
    }
  }

}
