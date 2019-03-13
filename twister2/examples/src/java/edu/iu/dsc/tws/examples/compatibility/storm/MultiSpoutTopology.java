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

package edu.iu.dsc.tws.examples.compatibility.storm;

import java.util.Collections;
import java.util.Map;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.twister2.Twister2StormWorker;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

/**
 * This is a basic example of a Storm topology.
 */
public final class MultiSpoutTopology extends Twister2StormWorker {

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(
        Collections.emptyMap()
    );

    JobConfig jobConfig = new JobConfig();

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("multi-source-example");
    jobBuilder.setWorkerClass(MultiSpoutTopology.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(1, 512, 1);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  @Override
  public StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word0", new TestWordSpout(), 2);
    builder.setSpout("word1", new TestWordSpout(), 2);
    builder.setSpout("word2", new TestWordSpout(), 2);
    builder.setBolt("exclaim1", new ExclamationBolt(), 2)
        .shuffleGrouping("word0")
        .shuffleGrouping("word1")
        .shuffleGrouping("word2");
    return builder.createTopology();
  }

  public static class TestWordSpout extends BaseRichSpout {

    private RandomString randomString;
    private SpoutOutputCollector spoutOutputCollector;
    private TopologyContext topologyContext;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      this.randomString = new RandomString(10);
      this.spoutOutputCollector = collector;
      this.topologyContext = context;
    }

    @Override
    public void nextTuple() {
      spoutOutputCollector.emit(new Values(randomString.nextString()
          + " from " + topologyContext.getThisComponentId(), System.currentTimeMillis()));
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "time"));
    }
  }

  public static class ExclamationBolt extends BaseRichBolt {
    private static final long serialVersionUID = 6945654705222426596L;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf,
                        TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
      System.out.println("Tuple received : " + tuple.getString(0)
          + " | sent at " + tuple.getLong(1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }
}
