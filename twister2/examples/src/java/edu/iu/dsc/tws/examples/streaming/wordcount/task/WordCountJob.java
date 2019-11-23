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
package edu.iu.dsc.tws.examples.streaming.wordcount.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.typed.streaming.SPartitionCompute;

/**
 * A simple wordcount program where fixed number of words are generated and the global counts
 * of words are calculated. Example is done using the compute API.
 */
public class WordCountJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(WordCountJob.class.getName());

  private static final String EDGE = "partition-edge";

  private static final int MAX_CHARS = 5;

  private static final int NO_OF_SAMPLE_WORDS = 100;

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID,
        workerController, persistentVolume, volatileVolume);

    // create source and aggregator
    WordSource source = new WordSource();
    WordAggregator counter = new WordAggregator();

    // build the graph
    ComputeGraphBuilder builder = ComputeGraphBuilder.newBuilder(config);
    builder.addSource("word-source", source, 4);
    builder.addCompute("word-aggregator", counter, 4)
        .partition("word-source")
        .viaEdge(EDGE)
        .withDataType(MessageTypes.OBJECT);
    builder.setMode(OperationMode.STREAMING);

    // build the graph
    ComputeGraph graph = builder.build();
    // execute graph
    cEnv.getTaskExecutor().execute(graph);
  }

  private static class WordSource extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;

    // sample words
    private List<String> sampleWords = new ArrayList<>();

    // the random used to pick he words
    private Random random;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.random = new Random();
      RandomString randomString = new RandomString(MAX_CHARS, random, RandomString.ALPHANUM);
      for (int i = 0; i < NO_OF_SAMPLE_WORDS; i++) {
        sampleWords.add(randomString.nextRandomSizeString());
      }
    }

    @Override
    public void execute() {
      String word = sampleWords.get(random.nextInt(sampleWords.size()));
      context.write(EDGE, word);
    }
  }

  private static class WordAggregator extends SPartitionCompute<String> {
    private static final long serialVersionUID = -254264903510284798L;

    // keep track of the counts
    private Map<String, Integer> counts = new HashMap<>();

    @Override
    public boolean partition(String word) {
      int count = 1;
      if (counts.containsKey(word)) {
        count = counts.get(word);
        count++;
      }
      counts.put(word, count);
      LOG.log(Level.INFO, String.format("%d Word %s count %s", context.globalTaskId(),
          word, count));
      return true;
    }
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("wordcount-streaming-task");
    jobBuilder.setWorkerClass(WordCountJob.class);
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
