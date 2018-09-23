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
package edu.iu.dsc.tws.examples.internal.task.streaming;

import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.internal.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.internal.task.TaskExamples;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;
import edu.iu.dsc.tws.task.streaming.BaseStreamSource;

public class STKeyedReduceExample extends BenchTaskWorker {
  private static final Logger LOG = Logger.getLogger(STKeyedReduceExample.class.getName());

  private static final String EDGE = "edge";

  private static int psource = 4;

  private static int psink = 1;

  private static final Op OPERATION = Op.SUM;

  private static final DataType DATA_TYPE = DataType.INTEGER;

  private static final DataType KEY_TYPE = DataType.OBJECT;

  @Override
  public void intialize() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    psource = taskStages.get(0);
    psink = taskStages.get(1);
    TaskExamples taskExamples = new TaskExamples();
    BaseStreamSource g = taskExamples.getStreamSourceClass("keyed-reduce", EDGE);
    BaseStreamSink r = taskExamples.getStreamSinkClass("keyed-reduce");
    taskGraphBuilder.addSource(SOURCE, g, psource);
    computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
    computeConnection.keyedReduce(SOURCE, EDGE, new IFunction() {
      @Override
      public Object onMessage(Object object1, Object object2) {
        return object1;
      }
    }, KEY_TYPE, DATA_TYPE);
  }
}
