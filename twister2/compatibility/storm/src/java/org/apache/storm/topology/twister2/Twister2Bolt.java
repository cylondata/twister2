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
package org.apache.storm.topology.twister2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;

public class Twister2Bolt implements ICompute, ISink {

  private IRichBolt stormBolt;
  private Twister2BoltDeclarer boltDeclarer;
  private Integer parallelism = 1;

  public Twister2Bolt(IRichBolt stormBolt,MadeASourceListener madeASourceListener) {
    this.stormBolt = stormBolt;
    this.boltDeclarer = new Twister2BoltDeclarer(madeASourceListener);
  }

  public Integer getParallelism() {
    return parallelism;
  }

  public void setParallelism(Integer parallelism) {
    this.parallelism = parallelism;
  }

  public Twister2BoltDeclarer getBoltDeclarer() {
    return boltDeclarer;
  }

  @Override
  public boolean execute(IMessage content) {
    return false;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    stormBolt.prepare(
        cfg.toMap(),
        new TopologyContext(context),
        new OutputCollector(context)
    );
  }
}
