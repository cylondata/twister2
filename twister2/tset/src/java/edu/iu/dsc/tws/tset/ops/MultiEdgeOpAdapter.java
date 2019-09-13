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
package edu.iu.dsc.tws.tset.ops;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;

public class MultiEdgeOpAdapter {
  private TaskContext taskContext;
  private int inEdgeCount;
  private final List<String> outEdges;
  private EdgeWriter outEdgeWriter;

  public MultiEdgeOpAdapter(TaskContext taskContext) {
    this.taskContext = taskContext;
    // inedges would be null for source tasks. But the end needs to be written.
    // Hence the count is set to 1
    this.inEdgeCount = taskContext.getInEdges() != null ? taskContext.getInEdges().size() : 1;
    this.outEdges = new ArrayList<>(taskContext.getOutEdges().keySet());

    if (outEdges.size() == 1) {
      this.outEdgeWriter = new OneEdgeWriter();
    } else if (outEdges.size() == 2) {
      this.outEdgeWriter = new TwoEdgeWriter();
    } else {
      this.outEdgeWriter = new MultiEdgeWriter();
    }
  }

  public <T> void writeToEdges(T output) {
    outEdgeWriter.write(output);
  }

  public <K, V> void keyedWriteToEdges(K key, V value) {
    outEdgeWriter.keyedWriteToEdges(key, value);
  }

  /*
  When writing end to edges, we have to wait till taskInstance.execute is called from all the input
  edges of the task. This is monitored using the inEdgeCount.
   */
  public void writeEndToEdges() {
    if (taskContext.getOperationMode() == OperationMode.STREAMING) {
      return;
    }

    if (--inEdgeCount == 0) {
      outEdgeWriter.writeEnd();
    }
  }


  private interface EdgeWriter {
    <T> void write(T data);

    void writeEnd();

    <K, V> void keyedWriteToEdges(K key, V value);
  }

  private class OneEdgeWriter implements EdgeWriter {
    @Override
    public <T> void write(T data) {
      taskContext.write(outEdges.get(0), data);
    }

    @Override
    public void writeEnd() {
      taskContext.end(outEdges.get(0));
    }

    @Override
    public <K, V> void keyedWriteToEdges(K key, V value) {
      taskContext.write(outEdges.get(0), key, value);
    }
  }

  private class TwoEdgeWriter implements EdgeWriter {

    @Override
    public <T> void write(T data) {
      taskContext.write(outEdges.get(0), data);
      taskContext.write(outEdges.get(1), data);
    }

    @Override
    public void writeEnd() {
      taskContext.end(outEdges.get(0));
      taskContext.end(outEdges.get(1));
    }

    @Override
    public <K, V> void keyedWriteToEdges(K key, V value) {
      taskContext.write(outEdges.get(0), key, value);
      taskContext.write(outEdges.get(1), key, value);
    }
  }

  private class MultiEdgeWriter implements EdgeWriter {
    @Override
    public <T> void write(T data) {
      int i = 0;
      while (i < outEdges.size()) {
        taskContext.write(outEdges.get(i), data);
        i++;
      }
    }

    @Override
    public void writeEnd() {
      int i = 0;
      while (i < outEdges.size()) {
        taskContext.end(outEdges.get(i));
        i++;
      }
    }

    @Override
    public <K, V> void keyedWriteToEdges(K key, V value) {
      int i = 0;
      while (i < outEdges.size()) {
        taskContext.write(outEdges.get(i), key, value);
        i++;
      }
    }
  }
}
