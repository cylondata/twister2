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

import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class Twister2Spout implements ISource {

  private ISpout stormSpout; //todo need BaseRichSpout??
  //todo currently omitting declare output fields
  private Twister2SpoutDeclarer spoutDeclarer;

  public Twister2Spout(ISpout stormSpout) {
    this.stormSpout = stormSpout;
    this.spoutDeclarer = new Twister2SpoutDeclarer();
  }

  public Twister2SpoutDeclarer getSpoutDeclarer() {
    return spoutDeclarer;
  }

  @Override
  public void execute() {
    while (true) {
      stormSpout.nextTuple();
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    stormSpout.open(
        cfg.toMap(),
        new TopologyContext(context),
        new SpoutOutputCollector(context)
    );
  }
}
