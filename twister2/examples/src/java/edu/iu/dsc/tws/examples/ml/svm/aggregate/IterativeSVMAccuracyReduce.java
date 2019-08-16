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
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.ICollector;

public class IterativeSVMAccuracyReduce extends BaseSink<Double> implements ICollector<Double> {
  private static final long serialVersionUID = 4268361215513644139L;

  private static final Logger LOG = Logger.getLogger(IterativeSVMAccuracyReduce.class.getName());

  private double newAccuracy;

  private boolean debug = false;

  private boolean status = false;

  private OperationMode operationMode;

  public IterativeSVMAccuracyReduce(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public DataPartition<Double> get() {
    return new EntityPartition<>(context.taskIndex(), newAccuracy);
  }

  @Override
  public boolean execute(IMessage<Double> message) {
    if (message.getContent() == null) {
      LOG.info("Null Accuracy Object, Something Went Wrong !!!");
      this.status = false;
    } else {
      this.newAccuracy = message.getContent();
    }
    return true;
  }
}
