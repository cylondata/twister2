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

import java.util.logging.Logger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class Twister2Spout implements ISource, Twister2StormNode {

  private static final Logger LOG = Logger.getLogger(Twister2Spout.class.getName());

  private IRichSpout stormSpout;
  private Twister2SpoutDeclarer spoutDeclarer;

  private Integer parallelism = 1;

  private String id;

  private EdgeFieldMap outFieldsForEdge;

  private EdgeFieldMap keyedOutEdges;

  public Twister2Spout(String id, IRichSpout stormSpout) {
    this.id = id;
    this.stormSpout = stormSpout;
    this.spoutDeclarer = new Twister2SpoutDeclarer();
    this.outFieldsForEdge = new EdgeFieldMap(Utils.getDefaultStream(id));
    this.keyedOutEdges = new EdgeFieldMap(Utils.getDefaultStream(id));
    this.stormSpout.declareOutputFields(this.outFieldsForEdge);
  }

  public void setParallelism(Integer parallelism) {
    this.parallelism = parallelism;
  }

  public Integer getParallelism() {
    return parallelism;
  }

  public Twister2SpoutDeclarer getSpoutDeclarer() {
    return spoutDeclarer;
  }

  @Override
  public Fields getOutFieldsForEdge(String edge) {
    return this.outFieldsForEdge.get(edge);
  }

  @Override
  public void setKeyedOutEdges(String edge, Fields keys) {
    LOG.info(String.format("[Storm-Spout : %s] Setting out edge %s with keys %s", id, edge, keys));
    this.keyedOutEdges.put(edge, keys);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void execute() {
    this.stormSpout.nextTuple();
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    LOG.info("Preparing storm-spout : " + this.id);

    this.stormSpout.open(
        cfg.toMap(),
        new TopologyContext(context),
        new SpoutOutputCollector(
            this.id,
            context,
            this.outFieldsForEdge,
            this.keyedOutEdges
        )
    );
  }
}
