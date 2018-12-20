package edu.iu.dsc.tws.dashboard.data_models.composite_ids;

import java.io.Serializable;
import java.util.Objects;

public class NodeId implements Serializable {

  private static final long serialVersionUID = 1L;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeId nodeId = (NodeId) o;
    return Objects.equals(ip, nodeId.ip)
        && Objects.equals(dataCenter, nodeId.dataCenter)
        && Objects.equals(rack, nodeId.rack);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ip, dataCenter, rack);
  }
}
