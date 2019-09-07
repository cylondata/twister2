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
package edu.iu.dsc.tws.python;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.python.processors.PythonClassProcessor;
import edu.iu.dsc.tws.python.processors.PythonLambdaProcessor;
import edu.iu.dsc.tws.python.tset.PyTSetSource;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class Twister2Environment {

  private TSetEnvironment tSetEnvironment;

  Twister2Environment(TSetEnvironment tSetEnvironment) {
    this.tSetEnvironment = tSetEnvironment;
  }

  public int getWorkerId() {
    return this.tSetEnvironment.getWorkerID();
  }

  public Config getConfig() {
    return this.tSetEnvironment.getConfig();
  }

  public SourceTSet createSource(byte[] lambda, int parallelism) {
    PyTSetSource pyTSetSource = new PyTSetSource(lambda);
    while (pyTSetSource.hasNext()) {
      System.out.println(pyTSetSource.next());
    }
    return (SourceTSet) tSetEnvironment.createSource(pyTSetSource, parallelism);
  }

  public void executePyFunction(byte[] lambda, Object input) {
    PythonLambdaProcessor pythonLambdaProcessor = new PythonLambdaProcessor(lambda);
    System.out.println("Printing output in java side : "
        + pythonLambdaProcessor.invoke(input));
  }

  public void executePyObject(byte[] lambda, Object... input) {
    PythonClassProcessor pythonLambdaProcessor = new PythonClassProcessor(lambda);
    for (int i = 0; i < 10; i++) {
      System.out.println("Printing output in java side : "
          + pythonLambdaProcessor.invoke("proc", input));
    }
  }
}
