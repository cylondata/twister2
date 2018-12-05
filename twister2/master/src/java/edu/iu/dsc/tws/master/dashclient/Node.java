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

import com.fasterxml.jackson.annotation.JsonProperty;

public class Node {

  @JsonProperty
  private String ip;

  @JsonProperty
  private String rack;

  @JsonProperty
  private String dataCenter;

  // Getter Methods
  public String getDataCenter() {
    return dataCenter;
  }

  public String getIp() {
    return ip;
  }

  public String getRack() {
    return rack;
  }

  // Setter Methods
  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public void setRack(String rack) {
    this.rack = rack;
  }

  @Override
  public String toString() {
    return "\"node\": {"
        + "\"ip\": " + "\"" + ip + "\", "
        + "\"rack\": " + "\"" + rack + "\", "
        + "\"dataCenter\": " + "\"" + dataCenter
        + '}';
  }
}
