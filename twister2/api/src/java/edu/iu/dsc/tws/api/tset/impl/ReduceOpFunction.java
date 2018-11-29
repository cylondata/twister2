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
package edu.iu.dsc.tws.api.tset.impl;

import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.task.api.IFunction;

public class ReduceOpFunction<T> implements IFunction {

  private ReduceFunction<T> reduceFn;

  public ReduceOpFunction(ReduceFunction<T> reduceFn) {
    this.reduceFn = reduceFn;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object onMessage(Object object1, Object object2) {
    T t1 = (T) object1;
    T t2 = (T) object2;
    return reduceFn.reduce(t1, t2);
  }
}
