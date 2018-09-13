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
package edu.iu.dsc.tws.examples.verification;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.executor.core.OperationNames;

public class ExperimentVerification implements IVerification {

  private static final Logger LOG = Logger.getLogger(ExperimentVerification.class.getName());

  private ExperimentData experimentData;
  private String operationNames;

  public ExperimentVerification(ExperimentData experimentData, String operationNames) {
    this.experimentData = experimentData;
    this.operationNames = operationNames;
  }

  @Override
  public boolean isVerified() throws VerificationException {
    boolean isVerified = false;
    if (experimentData.getInput() instanceof int[] && experimentData.getOutput() instanceof int[]) {
      if (OperationNames.REDUCE.equals(this.operationNames)) {
        int sourceCount = experimentData.getTaskStages().get(0);
        int sinkCount = experimentData.getTaskStages().get(1);
        if (sourceCount < sinkCount) {
          throw new VerificationException("Invalid task stages : " + sourceCount + "," + sinkCount);
        } else {
          int[] input = (int[]) experimentData.getInput();
          int[] output = (int[]) experimentData.getOutput();
          Object[] res = Arrays.stream(input)
              .map(i -> i * sourceCount)
              .boxed()
              .collect(Collectors.toList())
              .toArray();
          String resString = Arrays
              .toString(Arrays.copyOfRange(res, 0, Math.min(res.length, 10)));
          LOG.info("Expected Result : " + resString);
          String outString = Arrays.toString(output);
          isVerified = resString.equals(outString);
        }

      }
    }

    if (OperationNames.ALLREDUCE.equals(this.operationNames)) {

      if (experimentData.getInput() instanceof int[]
          && experimentData.getOutput() instanceof int[]) {
        int sourceCount = experimentData.getTaskStages().get(0);
        int sinkCount = experimentData.getTaskStages().get(1);
        if (sourceCount != sinkCount) {
          throw new VerificationException("Invalid task stages : " + sourceCount + "," + sinkCount);
        } else {
          LOG.info("Current Worker : " + experimentData.getWorkerId()
              + "/" + experimentData.getNumOfWorkers());
          int[] input = (int[]) experimentData.getInput();
          int[] output = (int[]) experimentData.getOutput();
          Object[] res = Arrays.stream(input)
              .map(i -> i * sourceCount)
              .boxed()
              .collect(Collectors.toList())
              .toArray();
          String resString = Arrays
              .toString(Arrays.copyOfRange(res, 0, Math.min(res.length, 10)));
          String outString = Arrays.toString(output);
          isVerified = resString.equals(outString);
        }
      }
    }
    return isVerified;
  }


}
