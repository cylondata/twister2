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
import edu.iu.dsc.tws.api.tset.BaseSink;
import edu.iu.dsc.tws.api.tset.BaseSource;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetBatchWorker;
import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.api.tset.fn.HashingPartitioner;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.link.KeyedReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.GroupedTSet;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

public class TSetSimpleWordCount extends TSetBatchWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(TSetSimpleWordCount.class.getName());

  @Override
  public void execute(TwisterBatchContext tc) {
    int sourcePar = 4;
    int sinkPar = 1;

    BatchSourceTSet<WordCountPair> source = tc.createSource(
        new WordGenerator((int) config.get("NO_OF_SAMPLE_WORDS"), (int) config.get("MAX_CHARS")),
        sourcePar).setName("source");

    GroupedTSet<String, WordCountPair> groupedWords = source.groupBy(new HashingPartitioner<>(),
        (Selector<String, WordCountPair>) WordCountPair::getWord);

    KeyedReduceTLink<String, WordCountPair> keyedReduce =
        groupedWords.keyedReduce(
            (ReduceFunction<WordCountPair>) (t1, t2) ->
                new WordCountPair(t1.getWord(), t1.getCount() + t2.getCount())
        );

    keyedReduce.sink(new WordCountLogger(), sinkPar);
  }

  class WordCountLogger extends BaseSink<WordCountPair> {
    @Override
    public boolean add(WordCountPair value) {
      LOG.info(value.toString());
      return true;
    }
  }

  class WordGenerator extends BaseSource<WordCountPair> {
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
    public WordCountPair next() {
      return new WordCountPair(iter.next(), 1);
    }

    @Override
    public void prepare() {
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
