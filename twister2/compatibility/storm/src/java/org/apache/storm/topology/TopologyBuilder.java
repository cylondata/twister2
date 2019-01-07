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

package org.apache.storm.topology;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.twister2.Twister2Spout;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;

public class TopologyBuilder {

  private TaskGraphBuilder taskGraphBuilder;

  public TopologyBuilder() {
    this.taskGraphBuilder = TaskGraphBuilder.newBuilder(null);
  }

  public StormTopology createTopology() {
    return null;
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
    return null;
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelismHint) {
    return null;
  }

  public BoltDeclarer setBolt(String id, IWindowedBolt bolt) throws IllegalArgumentException {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IWindowedBolt bolt, Number parallelismHint) throws
      IllegalArgumentException {
    return null;
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout) {
    return setSpout(id, spout, null);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
    Twister2Spout twister2Spout = new Twister2Spout(spout);
    return twister2Spout.getSpoutDeclarer();
  }
}
