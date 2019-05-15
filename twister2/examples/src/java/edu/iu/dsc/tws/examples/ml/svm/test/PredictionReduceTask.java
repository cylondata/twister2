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
package edu.iu.dsc.tws.examples.ml.svm.test;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class PredictionReduceTask extends BaseSink implements Collector {

  private static final long serialVersionUID = -254264120110286748L;

  private static final Logger LOG = Logger.getLogger(PredictionReduceTask.class.getName());

  private double[] object;

  private double doubleObject;

  private boolean debug = false;

  private boolean status = false;

  private OperationMode operationMode;

  public PredictionReduceTask(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public DataPartition<?> get() {
    return new EntityPartition<>(this.context.taskIndex(), this.doubleObject);
  }

  @Override
  public boolean execute(IMessage message) {
    if (message.getContent() == null) {
      LOG.info("Something Went Wrong !!!");
      this.status = false;
    } else {
      if (message.getContent() instanceof double[]) {
        this.status = true;
        this.object = (double[]) message.getContent();
        if (debug) {
          LOG.log(Level.INFO, "Received Data from workerId: " + this.context.getWorkerId()
              + ":" + this.context.globalTaskId() + ":" + Arrays.toString(this.object));
        }
      } else if (message.getContent() instanceof Double) {
        this.status = true;
        this.doubleObject = (double) message.getContent();
      } else {
        LOG.severe("Something Went Wrong");
      }

      if (this.operationMode.equals(OperationMode.BATCH)) {
        // do batch based computation
        LOG.info("Batch Mode : " + this.doubleObject);
      }

      if (this.operationMode.equals(OperationMode.STREAMING)) {
        // do streaming based computation
        LOG.info("Streaming Mode : " + this.doubleObject);
      }
    }
    return this.status;
  }
}
