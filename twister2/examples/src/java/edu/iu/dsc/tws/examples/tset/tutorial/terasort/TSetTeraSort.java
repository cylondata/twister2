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
package edu.iu.dsc.tws.examples.tset.tutorial.terasort;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.examples.tset.BaseTSetBatchWorker;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;

public class TSetTeraSort extends BaseTSetBatchWorker {

  private static final Logger LOG = Logger.getLogger(TSetTeraSort.class.getName());

  private static final String PARAM_DATA_SIZE_GB = "PARAM_DATA_SIZE_GB";
  private static final String PARAM_PARALLELISM = "PARAM_PARALLELISM";

  public static final class ByteArrayComparator implements Comparator<byte[]> {

    private static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

    private ByteArrayComparator() {
    }

    public static ByteArrayComparator getInstance() {
      return INSTANCE;
    }

    @Override
    public int compare(byte[] left, byte[] right) {
      for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
        int a = left[i] & 0xff;
        int b = right[j] & 0xff;
        if (a != b) {
          return a - b;
        }
      }
      return left.length - right.length;
    }
  }

  @Override
  public void execute(BatchTSetEnvironment env) {
    final int parallelism = env.getConfig().getIntegerValue(PARAM_PARALLELISM);
    final int dataSize = env.getConfig().getIntegerValue(PARAM_DATA_SIZE_GB);

    KeyedSourceTSet<byte[], byte[]> keyedSource = env.createKeyedSource(
        new SourceFunc<Tuple<byte[], byte[]>>() {

          private Queue<byte[]> keys = new LinkedList<>();
          private byte[] data = new byte[90];

          @Override
          public void prepare(TSetContext context) {
            Arrays.fill(data, (byte) 1);
            Random random = new Random();
            int noOfTuples = (int) ((dataSize * 1024 * 1024 * 1024 * 1.0d) / parallelism / 100);
            if (context.getIndex() == 0) {
              LOG.info(noOfTuples + " tuples will be produced in each source");
            }
            for (int i = 0; i < noOfTuples; i++) {
              byte[] key = new byte[10];
              random.nextBytes(key);
              keys.add(key);
            }
          }

          @Override
          public boolean hasNext() {
            return !keys.isEmpty();
          }

          @Override
          public Tuple<byte[], byte[]> next() {
            return new Tuple<>(keys.poll(), data);
          }
        }, parallelism);

    keyedSource.keyedGather(new PartitionFunc<byte[]>() {

      protected int keysToOneTask;
      protected int[] destinationsList;

      @Override
      public void prepare(Set<Integer> sources, Set<Integer> destinations) {
        int totalPossibilities = 256 * 256; //considering only most significant bytes of array
        this.keysToOneTask = (int) Math.ceil(totalPossibilities / (double) destinations.size());
        this.destinationsList = new int[destinations.size()];
        int index = 0;
        for (int i : destinations) {
          destinationsList[index++] = i;
        }
        Arrays.sort(this.destinationsList);
      }

      int getIndex(byte[] array) {
        int key = ((array[0] & 0xff) << 8) + (array[1] & 0xff);
        return key / keysToOneTask;
      }

      @Override
      public int partition(int sourceIndex, byte[] val) {
        return this.destinationsList[this.getIndex(val)];
      }

      @Override
      public void commit(int source, int partition) {

      }
    }, (left, right) -> ByteArrayComparator.getInstance().compare(left, right))
        .useDisk()
        .forEach(new ApplyFunc<Tuple<byte[], Iterator<byte[]>>>() {

          private byte[] previousKey;

          @Override
          public void apply(Tuple<byte[], Iterator<byte[]>> data) {
            if (previousKey != null) {
              int compare = ByteArrayComparator.getInstance().compare(previousKey, data.getKey());
              if (compare > 0) {
                LOG.warning("Unsorted keys detected. TeraSort has failed. " + compare);
              }
            }
            previousKey = data.getKey();
          }
        });
  }

  public static void main(String[] args) {

    if (args.length < 2) {
      LOG.warning("Missing arguments. Expected <parallelism> <data size>");
    }

    int parallelism = Integer.parseInt(args[0]);
    int dataSize = Integer.parseInt(args[1]);

    LOG.info(String.format("Data Size : %d, Parallelism : %d", dataSize, parallelism));

    JobConfig jobConfig = new JobConfig();
    jobConfig.put(PARAM_PARALLELISM, parallelism);
    jobConfig.put(PARAM_DATA_SIZE_GB, dataSize);

    Twister2Job job = Twister2Job.newBuilder()
        .setJobName(TSetTeraSort.class.getName())
        .setConfig(jobConfig)
        .setWorkerClass(TSetTeraSort.class)
        .addComputeResource(1, 512, 4)
        .build();

    Twister2Submitter.submitJob(job);
  }
}
