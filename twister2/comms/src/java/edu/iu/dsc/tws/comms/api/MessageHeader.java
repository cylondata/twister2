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
  public static final int HEADER_SIZE = 20;
  /**
   * The source task id
   */
  private int sourceId;
  /**
   * The destination task id
   */
  private int destId;
  /**
   * The edge id
   */
  private int edge;

  /**
   * The last node we visited
   */
  private int lastNode;

  /**
   * Length of the message
   */
  private int length;

  /**
   * Set of properties
   */
  private Map<String, Object> properties = new HashMap<>();

  private MessageHeader(int sourceId, int destId, int edge, int length, int lastNode) {
    this.sourceId = sourceId;
    this.destId = destId;
    this.edge = edge;
    this.length = length;
    this.lastNode = lastNode;
  }

  private void set(int sourceId, int destId, int edge, int length, int lastNode) {
    this.sourceId = sourceId;
    this.destId = destId;
    this.edge = edge;
    this.length = length;
    this.lastNode = lastNode;
  }

  public Object getProperty(String property) {
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

  public int getLastNode() {
    return lastNode;
  }

  public int getLength() {
    return length;
  }

  public static Builder newBuilder(int sourceId, int destId, int edge, int length, int lastNode) {
    return new Builder(sourceId, destId, edge, length, lastNode);
  }

  public static class Builder {
    private MessageHeader header;

    private Builder(int sourceId, int destId, int edge, int length, int lastNode) {
      header = new MessageHeader(sourceId, destId, edge, length, lastNode);
    }

    public Builder reInit(int sourceId, int destId, int edge, int length, int lastNode) {
      header.set(sourceId, destId, edge, length, lastNode);
      header.properties.clear();
      return this;
    }

    public Builder lastNode(int last) {
      header.lastNode = last;
      return this;
    }

    /**
     * Add a key value pair to be sent with the message
     * @param property
     * @param value
     */
    public Builder addProperty(String property, String value) {
      header.properties.put(property, value);
      return this;
    }

    public MessageHeader build() {
      return header;
    }
  }
}
