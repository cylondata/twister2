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
package edu.iu.dsc.tws.examples.internal.task;

import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;

public class TaskExamples {
  private static final Logger LOG = Logger.getLogger(TaskExamples.class.getName());

  /**
   * Examples For batch and Streaming
   **/

  protected static class ReduceSourceTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public ReduceSourceTask() {

    }

    public ReduceSourceTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(this.edge, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(this.edge, val)) {
          count++;
        }
      }
    }
  }

  protected static class ReduceSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      if (count % 1 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Batch Reduce Message Received : " + Arrays.toString((int[]) object));
        }
      }

      return true;
    }
  }

  public BaseBatchSource getSourceClass(String example, String edge) {
    BaseBatchSource source = null;
    if ("reduce".equals(example)) {
      source = new ReduceSourceTask(edge);
    }
    return source;
  }

  public BaseBatchSink getSinkClass(String example) {
    BaseBatchSink sink = null;
    if ("reduce".equals(example)) {
      sink = new ReduceSinkTask();
    }
    return sink;
  }
}
