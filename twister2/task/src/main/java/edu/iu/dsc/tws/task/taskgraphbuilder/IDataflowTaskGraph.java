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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.Set;

/**
 * This is the main interface for Dataflow task graph.
 */
public interface IDataflowTaskGraph<TV, TE> extends ITaskGraph<TV, TE> {

  /**
   * This method is responsible for returning the number of inward directed edges for the task vertex 'TV'
   */
  int inDegreeOf(TV taskVertex);

  /**
   * This method returns the set of incoming task edges for the task vertex 'TV'
   */
  Set<TE> incomingTaskEdgesOf(TV taskVertex);

  /**
   * This method returns the set of outward task edges for the task vertex 'TV'
   */
  int outDegreeOf(TV taskVertex);

  /**
   * This method returns the set of outgoing task edges for the task vertex 'TV'
   */
  Set<TE> outgoingTaskEdgesOf(TV taskVertex);

}
