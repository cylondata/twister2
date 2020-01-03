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
package edu.iu.dsc.tws.examples.csv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.data.api.formatters.LocalCSVInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;

public class PointDataSource extends BaseSource implements Collector {
  private static final Logger LOG = Logger.getLogger(PointDataSource.class.getName());

  private static final long serialVersionUID = -1L;
  private DataSource<?, ?> source;
  private String edgeName;
  private String dataDirectory;
  private String inputKey;
  private int dimension;
  private double[][] dataPointsLocal;

  PointDataSource() {
  }

  PointDataSource(String edgename, String dataDirectory, String inputKey, int dim) {
    this.edgeName = edgename;
    this.dataDirectory = dataDirectory;
    this.inputKey = inputKey;
    this.dimension = dim;
  }

  /**
   * This method get the partitioned datapoints using the task index and write those values using
   * the respective edge name.
   */
  @Override
  public void execute() {
    InputSplit<?> inputSplit = source.getNextSplit(context.taskIndex());
    List<double[]> points = new ArrayList<>();
    while (inputSplit != null) {
      LOG.info("input split value:" + inputSplit);
      try {
        while (!inputSplit.reachedEnd()) {
          Object value = inputSplit.nextRecord(null);
          LOG.info("%%%%%%%%% object value:%%%%%%%%%%%%" + value);
          if (value != null) {
            double[] row = new double[dimension];
            String[] data = value.toString().split(",");
            for (int j = 0; j < dimension; j++) {
              row[j] = Double.parseDouble(data[j].trim());
            }
            points.add(row);
          }
        }
        LOG.info("context task index:" + context.taskIndex() + "\t" + points.size());
        inputSplit = source.getNextSplit(context.taskIndex());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to read the input", e);
      }
    }
    dataPointsLocal = new double[points.size()][];
    int i = 0;
    for (double[] d : points) {
      dataPointsLocal[i++] = d;
    }
    context.end(edgeName);
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(
        ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new LocalCSVInputPartitioner(
        new Path(dataDirectory), context.getParallelism(), cfg));
    /*this.source = runtime.createInput(cfg, context, new LocalTextInputPartitioner(
        new Path(dataDirectory), context.getParallelism(), cfg));*/
  }

  @Override
  public DataPartition<double[][]> get() {
    return new EntityPartition<>(dataPointsLocal);
  }

  @Override
  public IONames getCollectibleNames() {
    return IONames.declare(inputKey);
  }
}
