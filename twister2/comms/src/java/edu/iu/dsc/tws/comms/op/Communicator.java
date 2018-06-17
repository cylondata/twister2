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
package edu.iu.dsc.tws.comms.op;

import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;

public class Communicator {
  private static final Logger LOG = Logger.getLogger(Communicator.class.getName());

  private Set<Integer> sources;

  private Set<Integer> destinations;

  private TWSChannel channel;

  private MessageType dataType = MessageType.OBJECT;

  private MessageType keyType = MessageType.OBJECT;

  private MessageType receiveDataType = MessageType.OBJECT;

  private MessageType receiveKeyType = MessageType.OBJECT;

  public Communicator(TWSChannel channel) {
    this.channel = channel;
  }

  public void setSources(Set<Integer> sources) {
    this.sources = sources;
  }

  public void setDestinations(Set<Integer> destinations) {
    this.destinations = destinations;
  }

  public void setDataType(MessageType dataType) {
    this.dataType = dataType;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }

  public void setReceiveDataType(MessageType receiveDataType) {
    this.receiveDataType = receiveDataType;
  }

  public void setReceiveKeyType(MessageType receiveKeyType) {
    this.receiveKeyType = receiveKeyType;
  }

  public Set<Integer> getSources() {
    return sources;
  }

  public int getSource() {
    return sources.iterator().next();
  }

  public Set<Integer> getDestinations() {
    return destinations;
  }

  public int getTarget() {
    return destinations.iterator().next();
  }

  public TWSChannel getChannel() {
    return channel;
  }

  public MessageType getDataType() {
    return dataType;
  }

  public MessageType getKeyType() {
    return keyType;
  }

  public MessageType getReceiveDataType() {
    return receiveDataType;
  }

  public MessageType getReceiveKeyType() {
    return receiveKeyType;
  }
}
