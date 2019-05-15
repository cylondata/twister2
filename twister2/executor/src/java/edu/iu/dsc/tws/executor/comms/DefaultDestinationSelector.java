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
package edu.iu.dsc.tws.executor.comms;

import java.util.Set;

import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.task.api.TaskPartitioner;

public class DefaultDestinationSelector implements DestinationSelector {
  private TaskPartitioner partitioner;

  public DefaultDestinationSelector(TaskPartitioner partitioner) {
    this.partitioner = partitioner;
  }

  @Override
  public void prepare(Communicator comm, Set<Integer> sources, Set<Integer> destinations) {
    partitioner.prepare(sources, destinations);
  }

  @Override
  public int next(int source, Object data) {
    return partitioner.partition(source, data);
  }

  @Override
  public void commit(int source, int next) {
    partitioner.commit(source, next);
  }

  @Override
  public int next(int source, Object key, Object data) {
    return partitioner.partition(source, key);
  }
}
