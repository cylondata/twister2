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

public final class MessageHeader {
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
   * Weather this originated from a sub node
   */
  private boolean subNodeOrigin;

  /**
   * Weather this is destined to a sub node
   */
  private boolean subNodeDestination;

  /**
   * Different paths for grouped collectives
   */
  private int path;

  /**
   * Set of properties
   */
  private Map<String, Object> properties = new HashMap<>();

  private MessageHeader(int srcId, int dstId, int e, int l, int lNode) {
    this.sourceId = srcId;
    this.destId = dstId;
    this.edge = e;
    this.length = l;
    this.lastNode = lNode;
  }

  private void set(int srcId, int dstId, int e, int l, int lNode) {
    this.sourceId = srcId;
    this.destId = dstId;
    this.edge = e;
    this.length = l;
    this.lastNode = lNode;
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

  public int getPath() {
    return path;
  }

  public boolean isSubNodeOrigin() {
    return subNodeOrigin;
  }

  public boolean isSubNodeDestination() {
    return subNodeDestination;
  }

  public static Builder newBuilder(int srcId, int dstId, int e, int l, int lNode) {
    return new Builder(srcId, dstId, e, l, lNode);
  }

  public static final class Builder {
    private MessageHeader header;

    private Builder(int sourceId, int destId, int edge, int length, int lastNode) {
      header = new MessageHeader(sourceId, destId, edge, length, lastNode);
    }

    public Builder reInit(int sourceId, int destId, int edge, int length, int lastNode) {
      header.set(sourceId, destId, edge, length, lastNode);
      header.subNodeDestination = false;
      header.subNodeOrigin = false;
      header.properties.clear();
      return this;
    }

    public Builder lastNode(int last) {
      header.lastNode = last;
      return this;
    }

    public Builder subNodeOrigin(boolean origin) {
      header.subNodeOrigin = origin;
      return this;
    }

    public Builder subNodeDestination(boolean destination) {
      header.subNodeDestination = destination;
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

    public Builder path(int p) {
      header.path = p;
      return this;
    }

    public MessageHeader build() {
      return header;
    }
  }
}
