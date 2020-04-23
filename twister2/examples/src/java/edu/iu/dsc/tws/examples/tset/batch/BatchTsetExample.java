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

package edu.iu.dsc.tws.examples.tset.batch;

import java.io.Serializable;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public abstract class BatchTsetExample implements BatchTSetIWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(BatchTsetExample.class.getName());

  static final int COUNT = 5;
  static final int PARALLELISM = 2;

  SourceTSet<Integer> dummySource(BatchTSetEnvironment env, int count, int parallel) {
    return env.createSource(new SourceFunc<Integer>() {
      private int c = 0;

      @Override
      public boolean hasNext() {
        return c < count;
      }

      @Override
      public Integer next() {
        return c++;
      }
    }, parallel);
  }

  SourceTSet<Integer> dummySource(BatchTSetEnvironment env, int start, int count, int parallel) {
    return env.createSource(new SourceFunc<Integer>() {
      private int c = start;
      private int limit = c + count;

      @Override
      public boolean hasNext() {
        return c < limit;
      }

      @Override
      public Integer next() {
        return c++;
      }
    }, parallel);
  }

  SourceTSet<String> dummyStringSource(BatchTSetEnvironment env, int count, int parallel) {
    return env.createSource(new SourceFunc<String>() {
      private int c = 0;
      private String[] dataList = {"The", "at", "one", "two", "three"};

      @Override
      public boolean hasNext() {
        return c < count;
      }

      @Override
      public String next() {
        return dataList[c++ % dataList.length];
      }
    }, parallel).withSchema(PrimitiveSchemas.STRING);
  }

  KeyedSourceTSet<String, Integer> dummyKeyedSource(BatchTSetEnvironment env, int count,
                                                    int parallel) {
    return env.createKeyedSource(new SourceFunc<Tuple<String, Integer>>() {
      private int c = 0;

      @Override
      public boolean hasNext() {
        return c < count;
      }

      @Override
      public Tuple<String, Integer> next() {
        c++;
        return new Tuple<>(Integer.toString(c), c);
      }
    }, parallel).withSchema(new KeyedSchema(MessageTypes.STRING, MessageTypes.INTEGER));
  }

  SourceTSet<Integer> dummyReplayableSource(BatchTSetEnvironment env, int count, int parallel) {
    return env.createSource(new SourceFunc<Integer>() {
      private int c = 0;

      @Override
      public boolean hasNext() {
        if (c == count) {
          c = 0;
          return false;
        }
        return c < count;
      }

      @Override
      public Integer next() {
        return c++;
      }
    }, parallel);
  }


  SourceTSet<Integer> dummySourceOther(BatchTSetEnvironment env, int count, int parallel) {
    return env.createSource(new SourceFunc<Integer>() {
      private int c = 25;

      @Override
      public boolean hasNext() {
        return c < count + 25;
      }

      @Override
      public Integer next() {
        return c++;
      }
    }, parallel).withSchema(PrimitiveSchemas.INTEGER);
  }

  KeyedSourceTSet<String, Integer> dummyKeyedSourceOther(BatchTSetEnvironment env, int count,
                                                         int parallel) {
    return env.createKeyedSource(new SourceFunc<Tuple<String, Integer>>() {
      private int c = 0;

      @Override
      public boolean hasNext() {
        return c < count;
      }

      @Override
      public Tuple<String, Integer> next() {
        c++;
        return new Tuple<>(Integer.toString(c), c + 25);
      }
    }, parallel).withSchema(new KeyedSchema(MessageTypes.STRING, MessageTypes.INTEGER));
  }

  public static void submitJob(Config config, int containers, JobConfig jobConfig, String clazz) {
    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(clazz.substring(clazz.lastIndexOf(".") + 1))
        .setWorkerClass(clazz)
        .addComputeResource(1, 512, containers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2JobState state = Twister2Submitter.submitJob(twister2Job, config);
  }


}
