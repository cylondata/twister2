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
package edu.iu.dsc.tws.examples.internal.task.streaming;

import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.internal.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.internal.task.TaskExamples;
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;
import edu.iu.dsc.tws.task.streaming.BaseStreamSource;

public class STPartitionExample extends BenchTaskWorker {
  private static final Logger LOG = Logger.getLogger(STPartitionExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int psource = taskStages.get(0);
    int psink = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    TaskExamples taskExamples = new TaskExamples();
    BaseStreamSource g = taskExamples.getStreamSourceClass("partition", edge);
    BaseStreamSink r = taskExamples.getStreamSinkClass("partition");
    taskGraphBuilder.addSource(SOURCE, g, psource);
    computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
    computeConnection.partition(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
}
