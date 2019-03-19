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

public class TSetKeyedGatherExample extends BaseTSetBatchWorker {
  private static final Logger LOG = Logger.getLogger(TSetKeyedReduceExample.class.getName());

  @Override
  public void execute(TwisterBatchContext tc) {
    super.execute(tc);

    // set the parallelism of source to task stage 0
    /*TSet<int[]> source = tSetBuilder.createSource(new TestBaseSource()).setName("Source").
        setParallelism(jobParameters.getTaskStages().get(0));
    TSet<int[]> reduce = source.groupBy(new LoadBalancePartitioner<>(), new IdentitySelector<>()).
        setParallelism(10);
    reduce.sink(new Sink<int[]>() {
      @Override
      public boolean add(int[] value) {
        experimentData.setOutput(value);
        try {
          verify(OperationNames.REDUCE);
        } catch (VerificationException e) {
          LOG.info("Exception Message : " + e.getMessage());
        }
        return true;
      }

      @Override
      public void prepare(TSetContext context) {
      }
    });
*/
  }

}
