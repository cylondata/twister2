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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * This class is responsible for handling the data objects and converting into two-dimensional array
 * of objects.
 */
public class DataObjectCompute extends BaseCompute {

  private static final Logger LOG = Logger.getLogger(DataObjectCompute.class.getName());
  private static final long serialVersionUID = -254264120110286748L;
  private int parallel;
  private int datasize;
  private int dimension;

  public DataObjectCompute(int dsize, int parallel, int dim) {
    this.parallel = parallel;
    this.datasize = dsize;
    this.dimension = dim;
  }

  public DataObjectCompute(int size, int dim) {
    this.datasize = size;
    this.dimension = dim;
  }

  public int getDatasize() {
    return datasize;
  }

  public void setDatasize(int datasize) {
    this.datasize = datasize;
  }

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  public int getParallel() {
    return parallel;
  }

  public void setParallel(int parallel) {
    this.parallel = parallel;
  }

  @Override
  public boolean execute(IMessage message) {
    if (message.getContent() instanceof Iterator) {
      int value = 0;
      double[][] datapoint;
      if (getParallel() > 0) {
        datapoint = new double[getDatasize() / getParallel() + 1][getDimension()];
      } else {
        datapoint = new double[getDatasize()][getDimension()];
      }
      while (((Iterator) message.getContent()).hasNext()) {
        String val = String.valueOf(((Iterator) message.getContent()).next());
        String[] data = val.split(",");
        for (int i = 0; i < getDimension(); i++) {
          datapoint[value][i] = Double.parseDouble(data[i].trim());
        }
        value++;
        context.write(Context.TWISTER2_DIRECT_EDGE, datapoint);
      }
    }
    context.end(Context.TWISTER2_DIRECT_EDGE);
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }
}
