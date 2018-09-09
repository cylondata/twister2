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
package edu.iu.dsc.tws.executor.threading;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;

public class StreamingShareingExecutor extends ThreadSharingExecutor {
  private static final Logger LOG = Logger.getLogger(StreamingShareingExecutor.class.getName());

  private int workerId;

  public StreamingShareingExecutor(int workerId) {
    this.workerId = workerId;
  }

  public boolean runExecution() {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    for (INodeInstance node : tasks) {
      node.prepare();
    }

    for (int i = 0; i < numThreads; i++) {
      Thread t = new Thread(new StreamWorker());
      t.setName("Thread-" + i);
      threads.add(t);
      t.start();
    }

    progressStreamComm();
    return true;
  }

  @Override
  public void stop(ExecutionPlan execution) {

  }

  protected class StreamWorker implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          INodeInstance nodeInstance = tasks.poll();
          if (nodeInstance != null) {
            nodeInstance.execute();
            tasks.offer(nodeInstance);
          }
        } catch (Throwable t) {
          LOG.log(Level.SEVERE, String.format("%d Error in executor", workerId), t);
        }
      }
    }
  }
}
