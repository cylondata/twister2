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
package edu.iu.dsc.tws.examples.tset.batch;

import java.io.Serializable;

import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSetBuilder;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.examples.comms.JobParameters;

public class BaseTSetWorker extends TaskWorker implements Serializable {
  protected static JobParameters jobParameters;

  protected TSetBuilder tSetBuilder;

  @Override
  public void execute() {
    tSetBuilder = TSetBuilder.newBuilder(config);
    jobParameters = JobParameters.build(config);
  }

  public static class BaseSource implements Source<int[]> {
    private int count = 0;

    @Override
    public boolean hasNext() {
      return count < 1;
    }

    @Override
    public int[] next() {
      count++;
      return new int[]{1, 1, 1};
    }

    @Override
    public void prepare(TSetContext context) {
    }
  }
}
