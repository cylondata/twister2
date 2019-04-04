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

package edu.iu.dsc.tws.examples.compatibility.storm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
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

public final class SentenceWordCountTopology extends Twister2StormWorker {

  /**
   * A spout that emits a random word
   */
  public static class FastRandomSentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 8068075156393183973L;

    private static final int ARRAY_LENGTH = 128 * 1024;
    private static final int WORD_LENGTH = 20;
    private static final int SENTENCE_LENGTH = 10;

    // Every sentence would be words generated randomly, split by space
    private final String[] sentences = new String[ARRAY_LENGTH];

    private final Random rnd = new Random(31);

    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      RandomString randomString = new RandomString(WORD_LENGTH);
      Random random = new Random();

      List<String> words = new ArrayList<>(10000);
      for (int i = 0; i < 10000; i++) {
        words.add(randomString.nextString());
      }

      for (int i = 0; i < ARRAY_LENGTH; i++) {
        StringBuilder sb = new StringBuilder();
        for (int j = 1; j < SENTENCE_LENGTH; j++) {
          sb.append(" ");
          sb.append(words.get(random.nextInt(10000)));
        }
        sentences[i] = sb.toString();
      }

      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      int nextInt = rnd.nextInt(ARRAY_LENGTH);
      collector.emit(new Values(sentences[nextInt]));
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static class SplitSentence extends BaseBasicBolt {
    private static final long serialVersionUID = 1249629174039601217L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String sentence = tuple.getString(0);
      System.out.println("Sentence received : " + sentence);
      for (String word : sentence.split("\\s+")) {
        collector.emit(new Values(word, 1));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static class WordCount extends BaseBasicBolt {
    private static final long serialVersionUID = -8492566595062774310L;

    private Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null) {
        count = 0;
      }
      count++;
      counts.put(word, count);
      System.out.println(word + ":" + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }


  @Override
  public StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();


    builder.setSpout("spout", new FastRandomSentenceSpout(), 1);
    builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 1)
        .fieldsGrouping("split", new Fields("word"));
    return builder.createTopology();
  }

  public static void main(String[] args) {
    String name = "fast-word-count-topology";
    if (args != null && args.length > 0) {
      name = args[0];
    }

    edu.iu.dsc.tws.common.config.Config config = ResourceAllocator.loadConfig(
        Collections.emptyMap()
    );

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName(name);
    jobBuilder.setWorkerClass(SentenceWordCountTopology.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(1, 512, 1);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
