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
package edu.iu.dsc.tws.examples.tset.checkpointing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.links.batch.KeyedReduceTLink;
import edu.iu.dsc.tws.tset.sets.batch.KeyedPersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.CheckpointingBatchTSetIWorker;

/**
 * A simple word count with checkpointing
 * we generate words randomly in-memory
 */
public class WordCount implements CheckpointingBatchTSetIWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(WordCount.class.getName());

  @Override
  @SuppressWarnings("RegexpSinglelineJava")
  public void execute(CheckpointingTSetEnv env) {
    int sourcePar = 4;
    Config config = env.getConfig();

    // create a source with fixed number of random words
    WordGenerator wordGenerator =
        new WordGenerator((int) config.get("NO_OF_SAMPLE_WORDS"), (int) config.get("MAX_CHARS"));
    SourceTSet<String> source = env.createSource(wordGenerator, sourcePar).setName("source");
    // persist raw data
    PersistedTSet<String> persisted = source.direct().persist();
    LOG.info("worker-" + env.getWorkerID() + " persisted initial raw data");

//    if (env.getWorkerID() == 1 && WorkerRuntime.getWorkerController().workerRestartCount() == 0) {
//      try {
//        Thread.sleep(6000);
//      } catch (InterruptedException e) {
//      }
//      throw new RuntimeException("intentionally killed");
//    }

    // map the words to a tuple, with <word, 1>, 1 is the count
    KeyedTSet<String, Integer> groupedWords = persisted.mapToTuple(w -> new Tuple<>(w, 1));
    // reduce using the sim operation
    KeyedReduceTLink<String, Integer> keyedReduce = groupedWords.keyedReduce(Integer::sum);

    // persist the counts
    KeyedPersistedTSet<String, Integer> persistedKeyedReduced = keyedReduce.persist();
    LOG.info("worker-" + env.getWorkerID() + " persisted keyedReduced data");

    // write to log for testing
    persistedKeyedReduced.keyedDirect().forEach(c -> LOG.info(c.toString()));
    if (env.getWorkerID() == 2
        && WorkerRuntime.getWorkerController().workerRestartCount() == 0
        && !CheckpointingContext.startingFromACheckpoint(config)) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
      }
      throw new RuntimeException("intentionally killed");
    }

  }

  /**
   * A simple source, that generates words
   */
  class WordGenerator extends BaseSourceFunc<String> {
    private Iterator<String> iter;
    // number of words to generate by each source
    private int count;
    // max characters in each word
    private int maxChars;

    WordGenerator(int count, int maxChars) {
      this.count = count;
      this.maxChars = maxChars;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public String next() {
      return iter.next();
    }

    @Override
    public void prepare(TSetContext ctx) {
      super.prepare(ctx);
      Random random = new Random();
      RandomString randomString = new RandomString(maxChars, random, RandomString.ALPHANUM);
      List<String> wordsList = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        wordsList.add(randomString.nextRandomSizeString());
      }
      // create a word list and use the iterator
      this.iter = wordsList.iterator();
    }
  }

  /**
   * We submit the job in the main method
   * @param args not using args for this job
   */
  public static void main(String[] args) {
    // build JobConfig, these are the parameters of the job
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("NO_OF_SAMPLE_WORDS", 100);
    jobConfig.put("MAX_CHARS", 5);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("wordcount");
    jobBuilder.setWorkerClass(WordCount.class);
    // we use 4 workers, each with 512mb memory and 1 CPU assigned
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}
