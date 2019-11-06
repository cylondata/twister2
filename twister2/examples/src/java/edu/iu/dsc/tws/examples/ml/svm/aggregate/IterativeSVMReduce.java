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
package edu.iu.dsc.tws.examples.ml.svm.aggregate;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;

public class IterativeSVMReduce extends BaseCompute implements Collector {

  private static final long serialVersionUID = -254264120110286748L;

  private static final Logger LOG = Logger.getLogger(IterativeSVMReduce.class.getName());

  private double[][] newWeightVector;

  private boolean debug = false;

  private boolean status = false;

  private OperationMode operationMode;

  public IterativeSVMReduce(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public DataPartition<?> get() {
    return new EntityPartition<>(context.taskIndex(), newWeightVector);
  }

  @Override
  public boolean execute(IMessage message) {

    if (message.getContent() == null) {
      LOG.info("Something Went Wrong !!!");
      this.status = false;
    } else {

      if (message.getContent() instanceof double[][]) {
        this.newWeightVector = (double[][]) message.getContent();
      }
    }
    return true;
  }
}
