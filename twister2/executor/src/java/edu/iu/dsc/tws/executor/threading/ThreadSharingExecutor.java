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
package edu.iu.dsc.tws.executor.threading;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.executor.ExecutionPlan;
import edu.iu.dsc.tws.executor.INodeInstance;

public class ThreadSharingExecutor extends ThreadExecutor {
  private static final Logger LOG = Logger.getLogger(ThreadSharingExecutor.class.getName());

  private int numThreads;

  private BlockingQueue<INodeInstance> tasks;

  private List<Thread> threads = new ArrayList<>();

  private ExecutionPlan executionPlan;

  public ThreadSharingExecutor() {
  }

  public ThreadSharingExecutor(int numThreads) {
    this.numThreads = numThreads;
  }

  public ThreadSharingExecutor(ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
  }

  @Override
  public void execute() {
    // go through the instances

    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    for (INodeInstance node : tasks) {
      node.prepare();
    }

    for (int i = 0; i < executionPlan.getNumThreads(); i++) {
      Thread t = new Thread(new Worker());
      t.start();
      threads.add(t);
    }
  }

  private class Worker implements Runnable {
    @Override
    public void run() {
      while (true) {
        INodeInstance nodeInstance = tasks.poll();
        nodeInstance.execute();
        tasks.offer(nodeInstance);
      }
    }
  }
}
