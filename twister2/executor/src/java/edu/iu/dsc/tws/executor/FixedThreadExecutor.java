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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class FixedThreadExecutor implements Runnable {
  private Map<String, BlockingQueue<IMessage>> inQueues = new HashMap<>();
  private Map<String, BlockingQueue<IMessage>> outQueues = new HashMap<>();
  private Table<String, String, DataFlowOperation> outOperations = HashBasedTable.create();

  private Map<String, ITask> tasks = new HashMap<>();
  private Map<String, ISource> sources = new HashMap<>();
  private DataFlowTaskGraph graph;

  @Override
  public void run() {
    for (Map.Entry<String, ITask> e : tasks.entrySet()) {
      BlockingQueue<IMessage> inQueue = inQueues.get(e.getKey());
      ITask task = e.getValue();
      if (inQueue != null) {
        while (!inQueue.isEmpty()) {
          IMessage m = inQueue.peek();
          task.execute(m);
        }
      } else {
        throw new RuntimeException("Task without an in stream:" + e.getKey());
      }

      BlockingQueue<IMessage> outQueue = outQueues.get(e.getKey());
      if (outQueue != null) {
        DataFlowOperation operation = outOperations.get(e.getKey(), e.getKey());
        while (!outQueue.isEmpty()) {
          // we need to construct the message
//          operation.send();
        }
      }
    }
  }
}
