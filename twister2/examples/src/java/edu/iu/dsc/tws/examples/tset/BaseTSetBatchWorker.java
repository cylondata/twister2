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

import java.io.Serializable;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSetBatchWorker;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.examples.comms.JobParameters;
import edu.iu.dsc.tws.examples.verification.ExperimentData;
import edu.iu.dsc.tws.examples.verification.ExperimentVerification;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * We need to keep variable static as this class is serialized
 */
public class BaseTSetBatchWorker extends TSetBatchWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(BaseTSetBatchWorker.class.getName());

  protected static JobParameters jobParameters;


  protected static ExperimentData experimentData;

  @Override
  public void execute(TwisterBatchContext tc) {
    jobParameters = JobParameters.build(config);

    experimentData = new ExperimentData();
    experimentData.setTaskStages(jobParameters.getTaskStages());
    if (jobParameters.isStream()) {
      throw new IllegalStateException("This worker does not support streaming, Please use"
          + "TSetStreamingWorker instead");
    } else {
      experimentData.setOperationMode(OperationMode.BATCH);
      experimentData.setIterations(jobParameters.getIterations());
    }
  }

  public static class TestBaseSource implements Source<int[]> {
    private int count = 0;

    private int[] values;

    @Override
    public boolean hasNext() {
      return count < jobParameters.getIterations();
    }

    @Override
    public int[] next() {
      count++;
      experimentData.setInput(values);
      return values;
    }

    @Override
    public void prepare(TSetContext context) {
      values = new int[jobParameters.getSize()];
      for (int i = 0; i < jobParameters.getSize(); i++) {
        values[i] = 1;
      }
    }
  }

  public static void verify(String operationNames) throws VerificationException {
    boolean doVerify = jobParameters.isDoVerify();
    boolean isVerified = false;
    if (doVerify) {
      LOG.info("Verifying results ...");
      ExperimentVerification experimentVerification
          = new ExperimentVerification(experimentData, operationNames);
      isVerified = experimentVerification.isVerified();
      if (isVerified) {
        LOG.info("Results generated from the experiment are verified.");
      } else {
        throw new VerificationException("Results do not match");
      }
    }
  }
}
