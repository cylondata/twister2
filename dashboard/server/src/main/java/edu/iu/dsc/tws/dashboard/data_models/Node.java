//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.dashboard.data_models;

import java.io.Serializable;

import javax.persistence.*;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import edu.iu.dsc.tws.dashboard.data_models.composite_ids.NodeId;
import io.swagger.annotations.ApiModelProperty;

@Entity
@IdClass(NodeId.class)
public class Node implements Serializable {

  @Id
  @Column(nullable = false)
  private String ip;

  @Id
  @Column
  private String rack = "default-rack";

  @Id
  @Column
  private String dataCenter = "default-datacenter";

  @ApiModelProperty(hidden = true) //till clusters are supported by twister2
  @ManyToOne(optional = false)
  @JoinColumn
  @JsonIgnoreProperties({"nodes", "description"})
  private Cluster cluster;

  public String getRack() {
    return rack;
  }

  public void setRack(String rack) {
    this.rack = rack;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }
}
