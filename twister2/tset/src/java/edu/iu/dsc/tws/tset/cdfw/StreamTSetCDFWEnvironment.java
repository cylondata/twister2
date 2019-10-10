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
package edu.iu.dsc.tws.tset.cdfw;

import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;
import edu.iu.dsc.tws.tset.env.BuildContext;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

public class StreamTSetCDFWEnvironment extends StreamingTSetEnvironment {

  private CDFWEnv cdfwEnv;

  public StreamTSetCDFWEnvironment(CDFWEnv env) {
    super();
    this.cdfwEnv = env;
  }

  @Override
  protected <T> DataObject<T> executeBuildContext(BuildContext buildContext,
                                                  BuildableTSet outputTset, boolean isIterative) {
    return super.executeBuildContext(buildContext, outputTset, isIterative);
  }

  @Override
  protected void pushInputsToFunctions(ComputeGraph graph, ExecutionPlan executionPlan) {
  }
}
