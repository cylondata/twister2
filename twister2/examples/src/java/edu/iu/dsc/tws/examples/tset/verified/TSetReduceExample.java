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
package edu.iu.dsc.tws.examples.tset.verified;

import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.link.batch.ReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.examples.tset.BaseTSetBatchWorker;
import edu.iu.dsc.tws.examples.verification.VerificationException;

public class TSetReduceExample extends BaseTSetBatchWorker {
  private static final Logger LOG = Logger.getLogger(TSetReduceExample.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    super.execute(env);

    // set the parallelism of source to task stage 0
    int srcPara = jobParameters.getTaskStages().get(0);
    int sinkPara = jobParameters.getTaskStages().get(1);
    SourceTSet<int[]> source = env.createSource(new TestBaseSource(), srcPara)
        .setName("Source");

    ReduceTLink<int[]> reduce = source.reduce((t1, t2) -> {
      int[] val = new int[t1.length];
      for (int i = 0; i < t1.length; i++) {
        val[i] = t1[i] + t2[i];
      }
      return val;
    });

    reduce.sink(value -> {
      experimentData.setOutput(value);
      LOG.info("Result : " + Arrays.toString(value));
      try {
        verify(OperationNames.REDUCE);
      } catch (VerificationException e) {
        LOG.info("Exception Message : " + e.getMessage());
      }
      return true;
    });
  }

}
