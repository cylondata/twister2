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
package edu.iu.dsc.tws.examples.internal.task.batch;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.internal.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;


public class BTReduceExample extends BenchTaskWorker {
  private static final Logger LOG = Logger.getLogger(BTReduceExample.class.getName());
  private static final String SOURCE = "source";
  private static final String SINK = "sink";
  private static final String EDGE = "edge";
  private static int psource = 4;
  private static int psink = 1;
  private static final Op OPERATION = Op.SUM;
  private static final DataType DATA_TYPE = DataType.INTEGER;

  @Override
  public void intialize() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    psource = taskStages.get(0);
    psink = taskStages.get(1);
    GeneratorTask g = new GeneratorTask();
    RecevingTask r = new RecevingTask();
    taskGraphBuilder.addSource(SOURCE, g, psource);
    computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
    computeConnection.reduce(SOURCE, EDGE, OPERATION, DATA_TYPE);
  }

  private static class GeneratorTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(EDGE, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(EDGE, val)) {
          count++;
        }
      }
    }
  }

  private static class RecevingTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      if (count % 1 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Received Message : " + Arrays.toString((int[]) object));
        }
      }

      return true;
    }
  }

}
