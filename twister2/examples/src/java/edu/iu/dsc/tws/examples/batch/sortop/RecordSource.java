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
package edu.iu.dsc.tws.examples.batch.sortop;

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.batch.BKeyedGather;

public class RecordSource implements Runnable {
  private static final Logger LOG = Logger.getLogger(RecordSource.class.getName());

  private BKeyedGather operation;

  private int taskId;

  private int executor;

  private long toSend;
  private byte[] value;
  private Random random;
  private int keySize;

  public RecordSource(Config cfg, int workerId, BKeyedGather op, int tIndex) {
    this.operation = op;
    this.taskId = tIndex;
    this.executor = workerId;

    int valueSize = cfg.getIntegerValue(SortJob.ARG_VALUE_SIZE, 90);
    this.keySize = cfg.getIntegerValue(SortJob.ARG_KEY_SIZE, 10);

    int noOfSources = cfg.getIntegerValue(SortJob.ARG_TASKS_SOURCES, 4);
    int totalSize = valueSize + keySize;
    this.toSend = (long) (cfg.getDoubleValue(
        SortJob.ARG_SIZE, 1.0
    ) * 1024 * 1024 * 1024 / totalSize / noOfSources);

    this.value = new byte[valueSize];
    Arrays.fill(this.value, (byte) 1);
    this.random = new Random(cfg.getIntegerValue(SortJob.ARG_KEY_SEED, 1000));

    if (workerId == 0) {
      LOG.info(String.format("Each source will send %d "
          + "messages of size %d bytes", this.toSend, totalSize));
    }
  }

  @Override
  public void run() {
    int count = 0;
    for (int i = 0; i < toSend; i++) {
      byte[] randomKey = new byte[this.keySize];
      this.random.nextBytes(randomKey);
      Tuple t = new Tuple(randomKey, this.value);
      while (!operation.gather(taskId, t, 0)) {
        count++;
        for (int j = 0; j < 4; j++) {
          operation.progressChannel();
        }
      }
    }
    LOG.info("Failed attempts: " + count);
    operation.finish(taskId);
  }
}
