//
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
package edu.iu.dsc.tws.comms.api;

import java.util.HashMap;
import java.util.Map;

public class MessageHeader {
  /**
   * The source task id
   */
  private final int sourceId;
  /**
   * The destination task id
   */
  private final int destId;
  /**
   * The edge id
   */
  private final int edge;

  /**
   * Set of properties
   */
  private Map<String, String> properties = new HashMap<>();

  public MessageHeader(int sourceId, int destId, int edge) {
    this.sourceId = sourceId;
    this.destId = destId;
    this.edge = edge;
  }

  /**
   * Add a key value pair to be sent with the message
   * @param property
   * @param value
   */
  public void addProperty(String property, String value) {

  }

  public String getProperty(String property) {
    return properties.get(property);
  }

  public int getSourceId() {
    return sourceId;
  }

  public int getDestId() {
    return destId;
  }

  public int getEdge() {
    return edge;
  }
}
