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

package edu.iu.dsc.tws.examples.compatibility.storm.windowing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.twister2.Twister2StormWorker;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

/**
 * This is an example of interval based tumbling windows on Twister2
 */
public final class IntervalWindowTopology extends Twister2StormWorker {

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(
        Collections.emptyMap()
    );

    JobConfig jobConfig = new JobConfig();
    jobConfig.put("topology.message.timeout.secs", 100000);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("sliding-window-example");
    jobBuilder.setWorkerClass(IntervalWindowTopology.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(1, 512, 1);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  @Override
  public StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("source", new TestWordSpout(), 1);
    builder.setBolt("windower", new IntervalWindowBolt()
        .withTumblingWindow(
            new BaseWindowedBolt.Duration(2, TimeUnit.SECONDS)
        ), 1).shuffleGrouping("source");
    return builder.createTopology();
  }

  public static class TestWordSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;

    private int counter = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      this.spoutOutputCollector = collector;
    }

    @Override
    public void nextTuple() {
      spoutOutputCollector.emit(new Values(counter++));
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("index"));
    }
  }

  public static class IntervalWindowBolt extends BaseWindowedBolt {

    private static final long serialVersionUID = 6945654705222426596L;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf,
                        TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(TupleWindow inputWindow) {
      List<Integer> indexesInThisWindow = new ArrayList<>();
      for (Tuple t : inputWindow.get()) {
        indexesInThisWindow.add(t.getInteger(0));
      }
      System.out.println("Tuple received : " + indexesInThisWindow);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }
}
