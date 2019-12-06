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

import java.util.Comparator;

import edu.iu.dsc.tws.python.processors.PythonLambdaProcessor;

public final class ComparatorFunctions extends TFunc<Comparator> {

  private static final ComparatorFunctions INSTANCE = new ComparatorFunctions();

  public static ComparatorFunctions getInstance() {
    return INSTANCE;
  }

  public static final class ComparatorImpl implements Comparator {

    private PythonLambdaProcessor lambdaProcessor;

    private ComparatorImpl(byte[] pyBinary) {
      this.lambdaProcessor = new PythonLambdaProcessor(pyBinary);
    }

    @Override
    public int compare(Object o, Object o1) {
      return ((Long) this.lambdaProcessor.invoke(o, o1)).intValue();
    }
  }

  @Override
  public Comparator build(byte[] pyBinary) {
    return new ComparatorImpl(pyBinary);
  }
}
