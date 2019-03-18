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

import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SVMCompute extends BaseCompute {

  private static final long serialVersionUID = -254264120110286748L;

  private static final Logger LOG = Logger.getLogger(SVMCompute.class.getName());

  private boolean debug = false;

  private double[] dataPoint;

  private OperationMode operationMode;

  public SVMCompute(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public boolean execute(IMessage content) {

    Object object = content.getContent();

    if (debug) {
      LOG.info("Message Type : " + content.getContent().getClass().getName());
    }

    if (this.operationMode.equals(OperationMode.BATCH)) {
      if (object instanceof Iterator) {
        while (((Iterator) object).hasNext()) {
          Object ret = ((Iterator) object).next();
          dataPoint = (double[]) ret;
          for (int i = 0; i < dataPoint.length; i++) {
            dataPoint[i] = dataPoint[i] / 4.0;
          }
          context.write(Constants.SimpleGraphConfig.REDUCE_EDGE, dataPoint);
        }
      }
      context.end(Constants.SimpleGraphConfig.REDUCE_EDGE);
    }

    if (this.operationMode.equals(OperationMode.STREAMING)) {
      if (object instanceof double[]) {
        this.dataPoint = (double[]) object;
        this.context.write(Constants.SimpleGraphConfig.REDUCE_EDGE, this.dataPoint);
        // do SVM-SGD computation
      }
    }

    return true;
  }
}
