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

import edu.iu.dsc.tws.api.tset.Collector;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.python.processors.PythonLambdaProcessor;

public class FlatMapFunctions extends TFunc<FlatMapFunc> {

  private static final FlatMapFunctions INSTANCE = new FlatMapFunctions();

  public static FlatMapFunctions getInstance() {
    return INSTANCE;
  }

  public static class FlatMapFuncImpl implements FlatMapFunc, Serializable {

    private PythonLambdaProcessor pythonLambdaProcessor;

    FlatMapFuncImpl(byte[] pyBinary) {
      this.pythonLambdaProcessor = new PythonLambdaProcessor(pyBinary);
    }

    @Override
    public void flatMap(Object o, Collector collector) {
      this.pythonLambdaProcessor.invoke(o, collector);
    }
  }

  @Override
  public FlatMapFunc build(byte[] pyBinary) {
    return new FlatMapFuncImpl(pyBinary);
  }
}
