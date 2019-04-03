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
package edu.iu.dsc.tws.examples.tset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
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
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class TSetSimpleWordCount extends TSetBatchWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(TSetSimpleWordCount.class.getName());

  @Override
  public void execute(TwisterBatchContext tc) {
    BatchSourceTSet<WordCountPair> source = tc.createSource(new WordSource(), 4)
        .setName("source");

    GroupedTSet<String, WordCountPair> groupedWords = source.groupBy(new HashingPartitioner<>(),
        (Selector<String, WordCountPair>) WordCountPair::getWord);

    KeyedReduceTLink<String, WordCountPair> keyedReduce =
        groupedWords.keyedReduce(
            (ReduceFunction<WordCountPair>) (t1, t2) ->
                new WordCountPair(t1.getWord(), t1.getCount() + t2.getCount())
        );

    keyedReduce.sink(new BaseSink<WordCountPair>() {
      @Override
      public void prepare() {
      }

      @Override
      public boolean add(WordCountPair value) {
        LOG.info(value.toString());
        return true;
      }
    }, 1);
  }

  static class WordCountPair implements Serializable {
    private static final long serialVersionUID = 64226990662415683L;

    private String word;
    private int count;

    WordCountPair() {
    }

    WordCountPair(String word, int count) {
      this.word = word;
      this.count = count;
    }

    public String getWord() {
      return word;
    }

    public int getCount() {
      return count;
    }

    @Override
    public String toString() {
      return word + ":" + count;
    }
  }

  static class WordSource extends BaseSource<WordCountPair> {
    private Iterator<String> iter;

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
      Config config = this.context.getConfig();

      Random random = new Random();
      int count = (int) config.get("NO_OF_SAMPLE_WORDS");
      int maxChars = (int) config.get("MAX_CHARS");

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
    jobBuilder.setJobName("tset-wordcount");
    jobBuilder.setWorkerClass(TSetSimpleWordCount.class);
    jobBuilder.addComputeResource(1, 512, 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}
