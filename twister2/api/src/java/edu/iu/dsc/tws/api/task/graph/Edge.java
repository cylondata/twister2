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
package edu.iu.dsc.tws.api.task.graph;

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.task.IFunction;
import edu.iu.dsc.tws.api.task.TaskPartitioner;

/**
 * Represents a edge in the graph
 */
public class Edge {
  /**
   * Name of the edge
   */
  private String name;

  /**
   * Optional function to apply for messages going through the edge
   */
  private IFunction function;

  /**
   * Partitioner
   */
  private TaskPartitioner partitioner;

  /**
   * The operation name of the edge
   */
  private String operation;

  /**
   * The data type that is flowing through the edge
   */
  private MessageType dataType = MessageTypes.OBJECT;

  /**
   * The key type flowing through the edge
   */
  private MessageType keyType = MessageTypes.OBJECT;

  /**
   * Weather we are a keyed
   */
  private boolean keyed = false;

  /**
   * Additional properties
   */
  private Map<String, Object> properties = new HashMap<>();

  /**
   * Multiple edges may be in a single operation. We need to configure the group
   */
  private String targetEdge;

  /**
   * The edge index
   */
  private int edgeIndex;

  /**
   * Number of edges for a particular operation
   */
  private int numberOfEdges;

  /**
   * ID generator for configuring comm ops
   */
  private EdgeID edgeID = new EdgeID();

  public Edge() {
  }

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

  public Edge(String name, String operation, MessageType dataType) {
    this.name = name;
    this.operation = operation;
    this.dataType = dataType;
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

  public Edge(String name, String operation, MessageType dataType,
              MessageType keyType, IFunction function, TaskPartitioner part) {
    this.name = name;
    this.function = function;
    this.operation = operation;
    this.dataType = dataType;
    this.keyType = keyType;
    this.keyed = true;
    this.partitioner = part;
  }

  /**
   * To get the name of the task edge
   */
  public String getName() {
    return name;
  }

  /**
   * To get the IFunction object.
   */
  public IFunction getFunction() {
    return function;
  }

  /**
   * To get the operation name
   */
  public String getOperation() {
    return operation;
  }

  /**
   * To get the datatype
   */
  public MessageType getDataType() {
    return dataType;
  }

  /**
   * To get the keytype
   */
  public MessageType getKeyType() {
    return keyType;
  }

  public boolean isKeyed() {
    return keyed;
  }

  /**
   * Add a property to the edge
   *
   * @param key key of the property
   * @param value value
   */
  public void addProperty(String key, Object value) {
    properties.put(key, value);
  }

  /**
   * Get the property with a specific key
   *
   * @param key name of the property
   * @return property if exists and null if not
   */
  public Object getProperty(String key) {
    return properties.get(key);
  }

  /**
   * Add the properties to the edge
   *
   * @param props properties
   */
  public void addProperties(Map<String, Object> props) {
    this.properties.putAll(props);
  }

  /**
   * Get the partitioner
   *
   * @return partitioner
   */
  public TaskPartitioner getPartitioner() {
    return partitioner;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setFunction(IFunction function) {
    this.function = function;
  }

  public void setPartitioner(TaskPartitioner partitioner) {
    this.partitioner = partitioner;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public void setDataType(MessageType dataType) {
    this.dataType = dataType;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }

  public void setKeyed(boolean keyed) {
    this.keyed = keyed;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  /**
   * Get the group name, if the group is set, multiple edges can belong to a same group
   *
   * @return the group
   */
  public String getTargetEdge() {
    return targetEdge;
  }

  public void setTargetEdge(String targetEdge) {
    this.targetEdge = targetEdge;
  }

  public void setEdgeIndex(int edgeIndex) {
    this.edgeIndex = edgeIndex;
  }

  public void setNumberOfEdges(int numberOfEdges) {
    this.numberOfEdges = numberOfEdges;
  }

  public int getEdgeIndex() {
    return edgeIndex;
  }

  public int getNumberOfEdges() {
    return numberOfEdges;
  }

  public EdgeID getEdgeID() {
    return edgeID;
  }
}
