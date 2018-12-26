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
package edu.iu.dsc.tws.api.htgjob;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

/**
 * This schedule is the base method for making decisions to run the part of the task graph which
 * will be improved further with the complex logic. Now, based on the relations(parent -> child)
 * it will initiate the execution.
 */
public class Twister2HTGScheduler implements ITwister2HTGScheduler {
  private static final Logger LOG = Logger.getLogger(Twister2HTGScheduler.class.getName());

  @Override
  public List<String> schedule(DataFlowTaskGraph twister2Metagraph) {
    LinkedList<String> scheduledGraph = new LinkedList<>();
    LOG.info("%%%% Scheduled Graph list details: %%%%" + scheduledGraph);
    return scheduledGraph;
  }
}

