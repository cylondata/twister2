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
package edu.iu.dsc.tws.api.tset;

import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;

public class BatchTSetEnvironment extends TSetEnvironment {

  BatchTSetEnvironment(WorkerEnvironment wEnv) {
    super(wEnv);
  }

  @Override
  public OperationMode getOperationMode() {
    return OperationMode.BATCH;
  }

  @Override
  public <T> BatchSourceTSet<T> createSource(SourceFunc<T> source, int parallelism) {
    BatchSourceTSet<T> sourceT = new BatchSourceTSet<>(this, source, parallelism);
    getGraph().addSourceTSet(sourceT);

    return sourceT;
  }
}
