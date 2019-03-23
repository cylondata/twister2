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

import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.IFunction;

public class PredictionAggregator implements IFunction {

  private static final long serialVersionUID = -254264120110286748L;

  private static final Logger LOG = Logger.getLogger(PredictionAggregator.class.getName());

  private Object aggregatorResult = null;

  @Override
  public Object onMessage(Object object1, Object object2) {
    if (object1 instanceof Double && object2 instanceof Double) {
      double v1 = (double) object1;
      double v2 = (double) object2;
      aggregatorResult = v1 + v2;
    } else {
      aggregatorResult = new double[]{1, 1, 1, 1, 1};
    }

    return aggregatorResult;
  }
}
