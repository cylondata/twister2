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
package edu.iu.dsc.tws.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.TWSChannel;

public class ThreadSharingExecutor {
  private static final Logger LOG = Logger.getLogger(ThreadSharingExecutor.class.getName());

  private int numThreads;

  private BlockingQueue<INodeInstance> tasks;

  private TWSChannel channel;

  private List<Thread> threads = new ArrayList<>();

  public ThreadSharingExecutor(int numThreads, TWSChannel channel) {
    this.numThreads = numThreads;
    this.channel = channel;
  }

  public void execute(Execution execution) {
    // go through the instances
    Map<Integer, INodeInstance> nodes = execution.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    for (int i = 0; i < numThreads; i++) {
      Thread t = new Thread(new Worker());
      t.start();
      threads.add(t);
    }

    // we need to progress the channel
    while (true) {
      channel.progress();
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
