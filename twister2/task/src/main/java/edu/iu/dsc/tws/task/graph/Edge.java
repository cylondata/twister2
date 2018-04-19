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

import edu.iu.dsc.tws.task.api.ITask;

public class Edge {
  public String name;
  public ITask function;
  public String operation;

  public Edge(String te) {
    this.name = te;
  }

  public Edge(String name, String operation) {
    this.name = name;
    this.operation = operation;
  }

  public Edge(String taskEdge, ITask function) {
    this.name = taskEdge;
    this.function = function;
  }

  public Edge(String name, String operation, ITask function) {
    this.name = name;
    this.function = function;
    this.operation = operation;
  }

  public String name() {
    return name;
  }

  public String getName() {
    return name;
  }

  public ITask function() {
    return function;
  }

  public String getOperation() {
    return operation;
  }

  public ITask getFunction() {
    return function;
  }
}
