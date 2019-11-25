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
package edu.iu.dsc.tws.examples.batch.wordcount.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
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
import edu.iu.dsc.tws.task.impl.function.ReduceFn;

/**
 * A simple wordcount program where fixed number of words are generated and the global counts
 * of words are calculated
 */
public class WordCountJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(WordCountJob.class.getName());

  private static final int NUMBER_MESSAGES = 100;

  private static final String EDGE = "reduce-edge";

  private static final int MAX_CHARS = 5;

  private static final int NO_OF_SAMPLE_WORDS = 100;

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);
    // source and aggregator
    WordSource source = new WordSource();
    WordAggregator counter = new WordAggregator();

    // build the task graph
    ComputeGraphBuilder builder = ComputeGraphBuilder.newBuilder(config);
    builder.addSource("word-source", source, 4);
    builder.addCompute("word-aggregator", counter, 4)
        .keyedReduce("word-source")
        .viaEdge(EDGE)
        .withReductionFunction(new ReduceFn(Op.SUM, MessageTypes.INTEGER_ARRAY))
        .withKeyType(MessageTypes.OBJECT)
        .withDataType(MessageTypes.INTEGER_ARRAY);

    builder.setMode(OperationMode.BATCH);

    // execute the graph
    ComputeGraph graph = builder.build();
    ExecutionPlan plan = cEnv.getTaskExecutor().plan(graph);
    cEnv.getTaskExecutor().execute(graph, plan);
  }

  private static class WordSource extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;

    private int count = 0;

    private List<String> sampleWords = new ArrayList<>();

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
      // indicate the end
      if (count == NUMBER_MESSAGES - 1) {
        if (context.writeEnd(EDGE, word, new int[]{1})) {
          count++;
        }
      } else if (count < NUMBER_MESSAGES - 1) {
        if (context.write(EDGE, word, new int[]{1})) {
          count++;
        }
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static class WordAggregator extends BaseCompute {
    private static final long serialVersionUID = -254264903510284798L;

    @Override
    public boolean execute(IMessage message) {
      // we get everything at once
      Iterator<Object> it;
      if (message.getContent() instanceof Iterator) {
        it = (Iterator<Object>) message.getContent();

        while (it.hasNext()) {
          Object next = it.next();
          if (next instanceof Tuple) {
            Tuple kc = (Tuple) next;
            LOG.log(Level.INFO, String.format("%d Word %s count %s",
                context.globalTaskId(), kc.getKey(), ((int[]) kc.getValue())[0]));
          }
        }
      }

      return true;
    }
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("wordcount-batch-task");
    jobBuilder.setWorkerClass(WordCountJob.class);
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
