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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * This schedule is the base method for making decisions to run the part of the task graph which
 * will be improved further with the complex logic. Now, based on the relations(parent -> child)
 * it will initiate the execution.
 */

public class Twister2HTGScheduler implements ITwister2HTGScheduler {

  private static final Logger LOG = Logger.getLogger(Twister2HTGScheduler.class.getName());

  @Override
  public List<String> schedule(Twister2Metagraph twister2Metagraph) {
    LinkedList<String> scheduledGraph = new LinkedList<>();

    if (twister2Metagraph.getRelation().size() == 1) {
      scheduledGraph.addFirst(twister2Metagraph.getRelation().iterator().next().getParent());
      scheduledGraph.addAll(Collections.singleton(
          twister2Metagraph.getRelation().iterator().next().getChild()));
    } else {
      int i = 0;
      while (i < twister2Metagraph.getRelation().size()) {
        scheduledGraph.addFirst(twister2Metagraph.getRelation().iterator().next().getParent());
        scheduledGraph.addAll(Collections.singleton(
            twister2Metagraph.getRelation().iterator().next().getChild()));
        i++;
      }
    }
    LOG.info("%%%% Scheduled Graph list details: %%%%" + scheduledGraph);
    return scheduledGraph;

  }
}

