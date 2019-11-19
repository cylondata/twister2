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
package org.apache.beam.runners.twister2;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.values.PCollectionView;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public class BeamBatchWorker implements Serializable, BatchTSetIWorker {

  private static final String SIDEINPUTS = "sideInputs";
  private static final String LEAVES = "leaves";

  @Override
  public void execute(BatchTSetEnvironment env) {
    Config config = env.getConfig();

    Map<PCollectionView<?>, BatchTSet<?>> sideInputs
        = (Map<PCollectionView<?>, BatchTSet<?>>) config.get(SIDEINPUTS);
    Set<TSet> leaves = (Set<TSet>) config.get(LEAVES);


  }
}
