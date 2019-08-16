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
package edu.iu.dsc.tws.examples.ml.svm.data;

import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.nodes.ISink;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.ICollector;
import edu.iu.dsc.tws.task.typed.AbstractIterableDataCompute;

public class IterativeSVMDataObjectDirectSink
    extends AbstractIterableDataCompute<double[][]> implements ISink, ICollector<double[][]> {

  private static final long serialVersionUID = 556286022242885874L;

  private static final Logger LOG = Logger.getLogger(IterativeSVMDataObjectDirectSink.class
      .getName());

  private double[][] dataPointsLocal;


  @Override
  public DataPartition<double[][]> get() {
    return new EntityPartition<>(context.taskIndex(), dataPointsLocal);
  }

  @Override
  public boolean execute(IMessage<Iterator<double[][]>> message) {
    Iterator<double[][]> itr = message.getContent();
    if (itr != null) {
      if (itr.hasNext()) {
        double[][] d = itr.next();
        if (d != null) {
          this.dataPointsLocal = d;
        } else {
          this.dataPointsLocal = null;
          LOG.severe(String.format("Something went wrong, Data Point is null"));
        }
      }
    }
    return true;
  }
}
