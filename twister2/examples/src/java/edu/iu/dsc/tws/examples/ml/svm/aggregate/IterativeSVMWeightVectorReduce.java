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
package edu.iu.dsc.tws.examples.ml.svm.aggregate;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.ICollector;

public class IterativeSVMWeightVectorReduce
    extends BaseCompute<double[]> implements ICollector<double[]> {

  private static final long serialVersionUID = -2284525532560239802L;
  private static final Logger LOG = Logger.getLogger(IterativeSVMWeightVectorReduce.class
      .getName());

  private double[] newWeightVector;

  private boolean debug = false;

  private OperationMode operationMode;

  public IterativeSVMWeightVectorReduce(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public boolean execute(IMessage<double[]> message) {

    if (message.getContent() == null) {
      LOG.info("Something Went Wrong !!!");
    } else {
      this.newWeightVector = message.getContent();

    }
    return true;
  }

  @Override
  public DataPartition<double[]> get() {
    return new EntityPartition<>(context.taskIndex(), newWeightVector);
  }
}
