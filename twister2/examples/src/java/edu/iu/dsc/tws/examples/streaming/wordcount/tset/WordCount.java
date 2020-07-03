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
package edu.iu.dsc.tws.examples.streaming.wordcount.tset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.fn.HashingPartitioner;

/**
 * TSet API based word count example. A simple wordcount program where fixed number of words
 * are generated and the global counts of words are calculated.
 */
public class WordCount implements IWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(WordCount.class.getName());

  private static final int MAX_CHARS = 5;

  private static final int NO_OF_SAMPLE_WORDS = 100;

  @Override
  public void execute(Config config, JobAPI.Job job, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    int workerId = workerController.getWorkerInfo().getWorkerID();
    StreamingEnvironment cEnv = TSetEnvironment.initStreaming(WorkerEnvironment.init(config,
        job, workerController, persistentVolume, volatileVolume));

    // create source and aggregator
    cEnv.createSource(new SourceFunc<String>() {
      // sample words
      private List<String> sampleWords = new ArrayList<>();
      // the random used to pick he words
      private Random random;

      @Override
      public void prepare(TSetContext context) {
        this.random = new Random();
        RandomString randomString = new RandomString(MAX_CHARS, random, RandomString.ALPHANUM);
        for (int i = 0; i < NO_OF_SAMPLE_WORDS; i++) {
          sampleWords.add(randomString.nextRandomSizeString());
        }
      }

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public String next() {
        return sampleWords.get(random.nextInt(sampleWords.size()));
      }
    }, 4).partition(new HashingPartitioner<>()).sink(new SinkFunc<String>() {
      // keep track of the counts
      private Map<String, Integer> counts = new HashMap<>();

      private TSetContext context;

      @Override
      public void prepare(TSetContext context) {
        this.context = context;
      }

      @Override
      public boolean add(String word) {
        int count = 1;
        if (counts.containsKey(word)) {
          count = counts.get(word);
          count++;
        }
        counts.put(word, count);
        LOG.log(Level.INFO, String.format("%d Word %s count %s", context.getIndex(),
            word, count));
        return true;
      }
    });

    // start executing the streaming graph
    cEnv.run();
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("wordcount-streaming-tset");
    jobBuilder.setWorkerClass(WordCount.class);
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
