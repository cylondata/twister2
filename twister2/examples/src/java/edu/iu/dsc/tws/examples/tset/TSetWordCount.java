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
import edu.iu.dsc.tws.api.tset.BaseSource;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetBatchWorker;
import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.OneToOnePartitioner;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.utils.RandomString;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class TSetWordCount extends TSetBatchWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(TSetWordCount.class.getName());

  @Override
  public void execute(TwisterBatchContext tc) {
    BatchSourceTSet<String> source = tc.createSource(new WordSource(), 1).setName("source");
    source.groupBy(new OneToOnePartitioner<>(), new Selector<Object, String>() {
      @Override
      public Object select(String t) {
        return null;
      }
    });
  }

  private static class WordSource extends BaseSource<String> {
    private List<String> wordsList = new ArrayList<>();
    private Iterator<String> iter;

    private WordSource() {
      Config config = this.context.getConfig();

      Random random = new Random();
      int count = (int) config.get("NO_OF_SAMPLE_WORDS");
      int maxChars = (int) config.get("MAX_CHARS");

      RandomString randomString = new RandomString(maxChars, random, RandomString.ALPHANUM);
      for (int i = 0; i < count; i++) {
        wordsList.add(randomString.nextRandomSizeString());
      }

      this.iter = wordsList.iterator();
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
    public void prepare() {

    }
  }

  public static void main(String[] args) {

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("NO_OF_SAMPLE_WORDS", 100);
    jobConfig.put("MAX_CHARS", 5);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("tset-wordcount");
    jobBuilder.setWorkerClass(TSetWordCount.class);
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}
