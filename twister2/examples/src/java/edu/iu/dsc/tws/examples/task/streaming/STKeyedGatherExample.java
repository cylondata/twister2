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
package edu.iu.dsc.tws.examples.task.streaming;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.typed.KeyedGatherCompute;

public class STKeyedGatherExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STKeyedGatherExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType keyType = DataType.OBJECT;
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseSource g = new KeyedSourceStreamTask(edge);
    ISink r = new KeyedGatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedGather(SOURCE, edge, keyType, dataType);
    return taskGraphBuilder;
  }

  protected static class KeyedGatherSinkTask extends KeyedGatherCompute<Object, int[]>
      implements ISink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean keyedGather(Iterator<Tuple<Object, int[]>> content) {
      while (content.hasNext()) {
        Tuple<Object, int[]> tuple = content.next();
        Object key = tuple.getKey();
        Object value = tuple.getValue();
        count++;
        if (count % jobParameters.getPrintInterval() == 0) {
          if (value instanceof Object[]) {
            Object[] objects = (Object[]) value;
            for (int i = 0; i < objects.length; i++) {
              int[] a = (int[]) objects[i];
              experimentData.setOutput(a);
              try {
                verify(OperationNames.KEYED_GATHER);
              } catch (VerificationException e) {
                LOG.info("Exception Message : " + e.getMessage());
              }
            }
          }
        }
      }
      return true;
    }
  }
}

//  protected static class KeyedGatherSinkTask extends BaseSink {
//    private static final long serialVersionUID = -254264903510284798L;
//    private int count = 0;
//
//    @Override
//    public boolean execute(IMessage message) {
//      Object object = message.getContent();
//      LOG.info("Message Keyed-Gather : " + message.getContent()
//          + ", Count : " + count);
//      if (object instanceof Iterator) {
//        Iterator<?> it = (Iterator<?>) object;
//        while (it.hasNext()) {
//          Object value = it.next();
//          LOG.info("Value : " + value.getClass().getName());
//          if (value instanceof Tuple) {
//            Tuple l = (Tuple) value;
//            Object key = l.getKey();
//            Object val = l.getValue();
//            LOG.info("Value : " + val.getClass().getName());
//            if (count % jobParameters.getPrintInterval() == 0) {
//              if (val instanceof int[]) {
//                int[] objects = (int[]) val;
//                if (count % jobParameters.getPrintInterval() == 0) {
//                  experimentData.setOutput(objects);
//                  try {
//                    verify(OperationNames.KEYED_GATHER);
//                  } catch (VerificationException e) {
//                    LOG.info("Exception Message : " + e.getMessage());
//                  }
//                }
//                  /*LOG.info("Keyed-Gathered Message , Key : " + key + ", Value : "
//                      + Arrays.toString(a));*/
//              }
//            }
//          }
//        }
//
//      }
//      return true;
//    }
//
