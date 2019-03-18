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

import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.IFunction;

public class ReduceAggregator implements IFunction {

  private static final long serialVersionUID = -254264120110286748L;

  private static final Logger LOG = Logger.getLogger(ReduceAggregator.class.getName());

  private double[] aggregatorResult = null;

  @Override
  public Object onMessage(Object object1, Object object2) {

    if (object1 instanceof double[] && object2 instanceof double[]) {
      double[] a1 = (double[]) object1;
      double[] a2 = (double[]) object2;
      aggregatorResult = new double[a2.length];
      Arrays.setAll(aggregatorResult, i -> a1[i] + a2[i]);
    }

    return aggregatorResult;
  }
}
