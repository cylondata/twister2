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
package edu.iu.dsc.tws.master.dashclient;

public class Node {
  private float id;
  private String host;
  private String os;
  private String heartbeatTime;
  private String state;
  private Cluster clusterObject;


  // Getter Methods

  public float getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public String getOs() {
    return os;
  }

  public String getHeartbeatTime() {
    return heartbeatTime;
  }

  public String getState() {
    return state;
  }

  public Cluster getCluster() {
    return clusterObject;
  }

  // Setter Methods

  public void setId(float id) {
    this.id = id;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setOs(String os) {
    this.os = os;
  }

  public void setHeartbeatTime(String heartbeatTime) {
    this.heartbeatTime = heartbeatTime;
  }

  public void setState(String state) {
    this.state = state;
  }

  public void setCluster(Cluster cluster) {
    this.clusterObject = cluster;
  }

  @Override
  public String toString() {
    return "Node{"
        + "id=" + id
        + ", host='" + host + '\''
        + ", os='" + os + '\''
        + ", heartbeatTime='" + heartbeatTime + '\''
        + ", state='" + state + '\''
        + ", clusterObject=" + clusterObject
        + '}';
  }
}
