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
package edu.iu.dsc.tws.task.graph;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.task.api.IFunction;

public class Edge {
  private String name;
  private IFunction function;
  private String operation;
  private MessageType dataType = MessageType.OBJECT;
  private MessageType keyType = MessageType.OBJECT;
  private boolean keyed = false;

  public Edge(String te) {
    this.name = te;
  }

  public Edge(String name, String operation) {
    this.name = name;
    this.operation = operation;
  }

  public Edge(String taskEdge, IFunction function) {
    this.name = taskEdge;
    this.function = function;
  }

  public Edge(String name, String operation, MessageType messageType) {
    this.name = name;
    this.operation = operation;
    this.dataType = messageType;
  }

  public Edge(String name, String operation, IFunction function) {
    this.name = name;
    this.function = function;
    this.operation = operation;
  }

  public Edge(String name, String operation, MessageType dataType, MessageType keyType) {
    this.name = name;
    this.operation = operation;
    this.dataType = dataType;
    this.keyType = keyType;
    this.keyed = true;
  }

  public Edge(String name, String operation, MessageType dataType,
              MessageType keyType, IFunction function) {
    this.name = name;
    this.function = function;
    this.operation = operation;
    this.dataType = dataType;
    this.keyType = keyType;
    this.keyed = true;
  }

  public Edge(String name, String operation, MessageType dataType, IFunction function) {
    this.name = name;
    this.function = function;
    this.operation = operation;
    this.dataType = dataType;
  }

  public String getName() {
    return name;
  }

  public IFunction getFunction() {
    return function;
  }

  public String getOperation() {
    return operation;
  }

  public MessageType getDataType() {
    return dataType;
  }

  public MessageType getKeyType() {
    return keyType;
  }

  public boolean isKeyed() {
    return keyed;
  }
}
