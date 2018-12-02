package edu.iu.dsc.tws.dashboard.data_models.composite_ids;

import java.io.Serializable;

public class NodeId implements Serializable {

  private String ip;
  private String dataCenter;
  private String rack;

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public String getRack() {
    return rack;
  }

  public void setRack(String rack) {
    this.rack = rack;
  }
}
