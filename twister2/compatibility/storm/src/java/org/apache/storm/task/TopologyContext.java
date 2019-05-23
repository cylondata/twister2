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

package org.apache.storm.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.topology.twister2.Twister2BoltDeclarer;
import org.apache.storm.topology.twister2.Twister2BoltGrouping;

import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * A TopologyContext is given to bolts and spouts in their "prepare" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the topology, such as task ids, inputs and outputs, etc.
 * <p>The TopologyContext is also used to declare ISubscribedState objects to
 * synchronize state with StateSpouts this object is subscribed to.
 */
public class TopologyContext {

  private TaskContext t2TaskContext;
  private Twister2BoltDeclarer boltDeclarer;

  public TopologyContext(TaskContext t2TaskContext) {
    this.t2TaskContext = t2TaskContext;
  }

  public int getThisTaskId() {
    return this.t2TaskContext.globalTaskId();
  }

  public String getThisComponentId() {
    return this.t2TaskContext.taskName();
  }

  /**
   * This method will be temporary used to extract parameter that are not yest supported by
   * {@link TaskContext}
   */
  public void setTempBoltDeclarer(Twister2BoltDeclarer t2bd) {
    this.boltDeclarer = t2bd;
  }

  /**
   * Gets the set of streams declared for the component of this task.
   */
  public Set<String> getThisStreams() {
    if (this.boltDeclarer == null) {
      throw new RuntimeException("Couldn't determine streams for this Node");
    }
    return this.boltDeclarer.getGroupings().stream()
        .map(Twister2BoltGrouping::getStreamId)
        .collect(Collectors.toSet());
  }

  /**
   * Gets the declared inputs to this component.
   * todo the return type of this method should be Map<GlobalStreamId, Grouping>, where
   * Grouping is a thrift object. So changing it to Object, just to support windowing.
   * {@link org.apache.storm.topology.WindowedBoltExecutor} just need the {@link GlobalStreamId}
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  public Map<GlobalStreamId, Object> getThisSources() {
    if (this.boltDeclarer == null) {
      throw new RuntimeException("Couldn't determine sources for this Node");
    }
    Map<GlobalStreamId, Object> sourcesMap = new HashMap<>();
    this.boltDeclarer.getGroupings().forEach(twister2BoltGrouping -> {
      GlobalStreamId globalStreamId = new GlobalStreamId();
      globalStreamId.set_componentId(twister2BoltGrouping.getComponentId());
      globalStreamId.set_streamId(twister2BoltGrouping.getStreamId());
      sourcesMap.put(globalStreamId, null);
    });
    return sourcesMap;
  }

  //todo implement other methods if required
}
