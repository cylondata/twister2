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

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.ICollector;

public class IterativeSVMDataPointReduce extends BaseSink<double[][]>
    implements ICollector<double[][]> {

  private static final long serialVersionUID = 5737384175970887837L;
  private static final Logger LOG = Logger.getLogger(IterativeSVMDataPointReduce.class.getName());

  private double[][] newDataPoint;

  private boolean debug = false;

  private boolean status = false;

  private OperationMode operationMode;

  public IterativeSVMDataPointReduce(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public DataPartition<double[][]> get() {
    return new EntityPartition<>(context.taskIndex(), newDataPoint);
  }

  @Override
  public boolean execute(IMessage<double[][]> message) {
    Object o = message.getContent();
    return true;
  }
}
