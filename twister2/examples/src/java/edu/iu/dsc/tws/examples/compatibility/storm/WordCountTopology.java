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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


/**
 * This is a topology that does simple word counts.
 */
public final class WordCountTopology extends Twister2StormWorker {

  @Override
  public StormTopology buildTopology() {
    int parallelism = config.getIntegerValue("parallelism", 1);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new WordSpout(), parallelism);
    builder.setBolt("consumer", new ConsumerBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));

    return builder.createTopology();
  }

  /**
   * A spout that emits a random word
   */
  public static class WordSpout extends BaseRichSpout {
    private static final long serialVersionUID = 4322775001819135036L;

    private static final int ARRAY_LENGTH = 128 * 1024;
    private static final int WORD_LENGTH = 20;

    private final String[] words = new String[ARRAY_LENGTH];

    private final Random rnd = new Random(31);

    private SpoutOutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      RandomString randomString = new RandomString(WORD_LENGTH);
      for (int i = 0; i < ARRAY_LENGTH; i++) {
        words[i] = randomString.nextString();
      }

      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      int nextInt = rnd.nextInt(ARRAY_LENGTH);
      collector.emit(new Values(words[nextInt]));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  public static class ConsumerBolt extends BaseRichBolt {
    private static final long serialVersionUID = -5470591933906954522L;

    private Map<String, Integer> countMap;

    @SuppressWarnings("rawtypes")
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
      countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
      String key = tuple.getString(0);
      int count = 1;
      if (countMap.get(key) == null) {
        countMap.put(key, count);
      } else {
        count = countMap.get(key);
        countMap.put(key, ++count);
      }
      System.out.println(key + ":" + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
  }

  /**
   * Main method
   */
  public static void main(String[] args) {

    int parallelism = 1;
    if (args.length > 1) {
      parallelism = Integer.parseInt(args[1]);
    }

    edu.iu.dsc.tws.common.config.Config config = ResourceAllocator.loadConfig(
        Collections.emptyMap()
    );

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("parallelism", parallelism);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("word-count-storm");
    jobBuilder.setWorkerClass(WordCountTopology.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(1, 512, 1);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
