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
package edu.iu.dsc.tws.python.tset.fn;

import java.io.Serializable;

import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.python.processors.PythonLambdaProcessor;

public class ReduceFunctions extends TFunc<ReduceFunc> {

  private static final ReduceFunctions INSTANCE = new ReduceFunctions();

  public static ReduceFunctions getInstance() {
    return INSTANCE;
  }

  public static class ReduceFuncImpl implements ReduceFunc, Serializable {

    private PythonLambdaProcessor pythonLambdaProcessor;

    public ReduceFuncImpl(byte[] pyBinary) {
      this.pythonLambdaProcessor = new PythonLambdaProcessor(pyBinary);
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return this.pythonLambdaProcessor.invoke(t1, t2);
    }
  }

  @Override
  public ReduceFunc build(byte[] pyBinary) {
    return new ReduceFuncImpl(pyBinary);
  }
}
