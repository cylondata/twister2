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
package edu.iu.dsc.tws.examples.comms;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageFlags;


/**
 * BenchWorker class that works with keyed join operations, The worker generates two sets
 * of data with a single key type for both
 */
public abstract class JoinedKeyedBenchWorker extends KeyedBenchWorker {
  private static final Logger LOG = Logger.getLogger(JoinedKeyedBenchWorker.class.getName());
  private Lock lock = new ReentrantLock();

  protected abstract boolean sendMessages(int task, Object key, Object data, int flag, int tag);

  protected class MapWorker implements Runnable {
    private int task;

    public MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker: " + workerId + " task: " + task);
      int[] dataLeft = DataGenerator.generateIntData(jobParameters.getSize());
      int[] dataRight = DataGenerator.generateIntData(jobParameters.getSize());
      Integer key;
      for (int i = 0; i < jobParameters.getIterations(); i++) {
        // lets generate a message
        key = 100 + task;
        int flag = 0;
        if (i == jobParameters.getIterations() - 1) {
          flag = MessageFlags.LAST;
        }
        sendMessages(task, key, dataLeft, flag, 0);
        sendMessages(task, key, dataRight, flag, 1);
      }
      LOG.info(String.format("%d Done sending", workerId));
      lock.lock();
      finishedSources.put(task, true);
      boolean allDone = true;
      for (Map.Entry<Integer, Boolean> e : finishedSources.entrySet()) {
        if (!e.getValue()) {
          allDone = false;
        }
      }
      finishCommunication(task);
      sourcesDone = allDone;
      lock.unlock();
//      LOG.info(String.format("%d Sources done %s, %b", id, finishedSources, sourcesDone));
    }
  }
}
