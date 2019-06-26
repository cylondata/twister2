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
package edu.iu.dsc.tws.examples.ml.svm.compute;

import java.util.logging.Logger;

import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.ICollector;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class IterativeStreamingCompute extends BaseSink<double[]> implements ICollector<double[]> {
  private static final long serialVersionUID = 332173590941256461L;
  private static final Logger LOG = Logger.getLogger(IterativeStreamingCompute.class.getName());

  private double[] newWeightVector;

  private boolean debug = false;

  private OperationMode operationMode;

  public IterativeStreamingCompute(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public DataPartition<double[]> get() {
    return new EntityPartition<>(context.taskIndex(), newWeightVector);
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
}
