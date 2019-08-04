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

package edu.iu.dsc.tws.examples.batch.wordcount.tset;

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
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.api.tset.link.batch.KeyedReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.api.tset.worker.BatchTSetIWorker;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

public class TSetSimpleWordCount implements BatchTSetIWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(TSetSimpleWordCount.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    int sourcePar = 4;
//    int sinkPar = 1;

    Config config = env.getConfig();

    SourceTSet<String> source = env.createSource(
        new WordGenerator((int) config.get("NO_OF_SAMPLE_WORDS"), (int) config.get("MAX_CHARS")),
        sourcePar).setName("source");

    KeyedTSet<String, Integer, String> groupedWords = source.mapToTuple(w -> new Tuple<>(w, 1));

    KeyedReduceTLink<String, Integer> keyedReduce = groupedWords.keyedReduce(Integer::sum);

    keyedReduce.forEach(c -> LOG.info(c.toString()));
  }

  class WordGenerator extends BaseSourceFunc<String> {
    private Iterator<String> iter;
    private int count;
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
      LOG.info("source: " + wordsList);
      this.iter = wordsList.iterator();
    }
  }

  public static void main(String[] args) {

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("NO_OF_SAMPLE_WORDS", 100);
    jobConfig.put("MAX_CHARS", 5);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("tset-simple-wordcount");
    jobBuilder.setWorkerClass(TSetSimpleWordCount.class);
    jobBuilder.addComputeResource(1, 512, 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}
