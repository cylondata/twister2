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
package edu.iu.dsc.tws.examples.batch.kmeansoptimization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class DataParallelSinkImpl extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(DataParallelSinkImpl.class.getName());

  private static final long serialVersionUID = -1L;

  private int dimension;
  private int count = 1;
  private double[][] datapoint;
  private DataObject<double[][]> datapoints;

  @Override
  public boolean execute(IMessage message) {
    int j = 0;
    LOG.log(Level.INFO, "worker id " + context.getWorkerId()
        + "\ttask id:" + context.taskIndex());
    Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>) message.getContent();
    while (arrayListIterator.hasNext()) {
      String value = String.valueOf(arrayListIterator.next());
      if (!"Finished".equals(value)) {
        count++;
      }
      datapoint = new double[count][dimension];
      StringTokenizer st = new StringTokenizer(value, ",");
      if (!st.nextToken().equals("Finished")) {
        String val = st.nextToken();
        datapoint[j][0] = Double.valueOf(val);
        datapoint[j][1] = Double.valueOf(val);
        j++;
        if (j == 40) {
          break;
        }
      }
      datapoints.addPartition(new EntityPartition<>(0, datapoint));
    }
    LOG.info("Total Elements to be processed:" + count + "\t" + Arrays.toString(datapoint));
    return true;
  }

  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    dimension = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_DIMENSIONS));
    datapoints = new DataObjectImpl<>(config);
  }

  @Override
  public DataPartition<double[][]> get() {
    return null;
  }
}
