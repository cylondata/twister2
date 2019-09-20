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

import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.python.processors.PythonLambdaProcessor;

public class ApplyFunctions extends TFunc<ApplyFunc> {

  private static final ApplyFunctions INSTANCE = new ApplyFunctions();

  public static ApplyFunctions getInstance() {
    return INSTANCE;
  }

  public static class ApplyFuncImpl implements ApplyFunc, Serializable {

    private PythonLambdaProcessor pythonLambdaProcessor;

    ApplyFuncImpl(byte[] pyBytes) {
      this.pythonLambdaProcessor = new PythonLambdaProcessor(pyBytes);
    }

    @Override
    public void apply(Object data) {
      this.pythonLambdaProcessor.invoke(data);
    }
  }


  @Override
  public ApplyFunc build(byte[] pyBinary) {
    return new ApplyFuncImpl(pyBinary);
  }
}
