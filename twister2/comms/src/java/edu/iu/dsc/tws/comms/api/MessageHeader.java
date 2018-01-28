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

/**
 * Information about the message. All this information doesn't have to be sent along the message and
 * some can be derived from other means. It is upto the implementations how to transfer this
 * information with the message.
 */
public final class MessageHeader {
  /**
   * The source task id where this message originated from
   */
  private int sourceId;

  /**
   * The edge id
   */
  private int edge;

  /**
   * Length of the message
   */
  private int length;

  /**
   * An integer value used to identify the destination
   */
  private int destinationIdentifier;

  /**
   * Different paths for grouped collectives
   */
  private int flags;

  private MessageHeader(int srcId, int e, int l) {
    this.sourceId = srcId;
    this.edge = e;
    this.length = l;
  }

  private MessageHeader(int srcId, int e) {
    this.sourceId = srcId;
    this.edge = e;
  }

  public int getSourceId() {
    return sourceId;
  }

  public int getEdge() {
    return edge;
  }

  public int getLength() {
    return length;
  }

  public int getFlags() {
    return flags;
  }

  public int getDestinationIdentifier() {
    return destinationIdentifier;
  }

  public static Builder newBuilder(int srcId, int e, int l) {
    return new Builder(srcId, e, l);
  }

  public static Builder newBuilder(int srcId, int e) {
    return new Builder(srcId, e);
  }

  public static final class Builder {
    private MessageHeader header;

    private Builder(int sourceId, int edge, int length) {
      header = new MessageHeader(sourceId, edge, length);
    }

    private Builder(int sourceId, int edge) {
      header = new MessageHeader(sourceId, edge);
    }

    public Builder reInit(int sourceId, int edge, int length) {
      header.sourceId = sourceId;
      return this;
    }

    public Builder destination(int edge) {
      header.destinationIdentifier = edge;
      return this;
    }

    public Builder flags(int p) {
      header.flags = p;
      return this;
    }

    public Builder length(int l) {
      header.length = l;
      return this;
    }

    public MessageHeader build() {
      return header;
    }
  }
}
