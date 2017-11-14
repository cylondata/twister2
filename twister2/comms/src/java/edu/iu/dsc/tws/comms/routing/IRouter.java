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
package edu.iu.dsc.tws.comms.routing;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IRouter {
  /**
   * Return the list of receiving executors for this
   * @return
   */
  Set<Integer> receivingExecutors();

  /**
   * Initialize the message receiver with tasks from which messages are expected
   * For each sub edge in graph, for each path, gives the expected task ids
   *
   * path -> (ids)
   *
   * @return  expected task ids
   */
  Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds();

  /**
   * Is this the final task
   * @return
   */
  boolean isLastReceiver();
  /**
   * Get routing for a source
   * @param source
   * @return
   */
  Map<Integer, Map<Integer, Set<Integer>>> getInternalSendTasks(int source);

  Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasks(int source);

  /**
   * Return the main task of an executor
   * @param executor
   * @return
   */
  int mainTaskOfExecutor(int executor);

  /**
   * Get a destination identifier, used for determining the correct task to hand over a message
   * @return
   */
  int destinationIdentifier(int source, int path);
}
