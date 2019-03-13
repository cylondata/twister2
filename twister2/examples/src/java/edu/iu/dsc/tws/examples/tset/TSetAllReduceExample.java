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
package edu.iu.dsc.tws.examples.tset;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.SourceTSet;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;

public class TSetAllReduceExample extends BaseTSetBatchWorker {
  private static final Logger LOG = Logger.getLogger(TSetAllReduceExample.class.getName());

  @Override
  public void execute(TwisterBatchContext tc) {
    super.execute(tc);

    // set the parallelism of source to task stage 0
    int srcPara = jobParameters.getTaskStages().get(0);
    int sinkPara = jobParameters.getTaskStages().get(1);
    SourceTSet<int[]> source = tc.createSource(new BaseSource(), srcPara).setName("Source");
    AllReduceTLink<int[]> reduce = source.allReduce((t1, t2) -> {
      int[] val = new int[t1.length];
      for (int i = 0; i < t1.length; i++) {
        val[i] = t1[i] + t2[i];
      }
      return val;
    });

    reduce.sink(value -> {
      experimentData.setOutput(value);
      try {
        verify(OperationNames.ALLREDUCE);
      } catch (VerificationException e) {
        LOG.info("Exception Message : " + e.getMessage());
      }
      return true;
    }, sinkPara);
  }

}
