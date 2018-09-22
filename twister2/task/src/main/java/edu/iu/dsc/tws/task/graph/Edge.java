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

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.api.IFunction;

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
   * The operation name of the edge
   */
  private String operation;

  /**
   * The data type that is flowing through the edge
   */
  private DataType dataType = DataType.OBJECT;

  /**
   * The key type flowing through the edge
   */
  private DataType keyType = DataType.OBJECT;

  /**
   * Weather we are a keyed
   */
  private boolean keyed = false;

  /**
   * Additional properties
   */
  private Map<String, Object> properties = new HashMap<>();

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

  public Edge(String name, String operation, DataType dataType) {
    this.name = name;
    this.operation = operation;
    this.dataType = dataType;
  }

  public Edge(String name, String operation, IFunction function) {
    this.name = name;
    this.function = function;
    this.operation = operation;
  }

  public Edge(String name, String operation, DataType dataType, DataType keyType) {
    this.name = name;
    this.operation = operation;
    this.dataType = dataType;
    this.keyType = keyType;
    this.keyed = true;
  }

  public Edge(String name, String operation, DataType dataType,
              DataType keyType, IFunction function) {
    this.name = name;
    this.function = function;
    this.operation = operation;
    this.dataType = dataType;
    this.keyType = keyType;
    this.keyed = true;
  }

  public Edge(String name, String operation, DataType dataType, IFunction function) {
    this.name = name;
    this.function = function;
    this.operation = operation;
    this.dataType = dataType;
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
  public DataType getDataType() {
    return dataType;
  }

  /**
   * To get the keytype
   */
  public DataType getKeyType() {
    return keyType;
  }

  public boolean isKeyed() {
    return keyed;
  }

  /**
   * Add a property to the edge
   * @param key key of the property
   * @param value value
   */
  public void addProperty(String key, Object value) {
    properties.put(key, value);
  }

  /**
   * Get the property with a specific key
   * @param key name of the property
   * @return property if exists and null if not
   */
  public Object getProperty(String key) {
    return properties.get(key);
  }

  /**
   * Add the properties to the edge
   * @param props properties
   */
  public void addProperties(Map<String, Object> props) {
    this.properties.putAll(props);
  }
}
