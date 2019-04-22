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
package edu.iu.dsc.tws.examples.ml.svm.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ResultsSaver {

  private double trainingTime;

  private double testingTime;

  private double dataLoadingTime;

  private double totalTime;

  private SVMJobParameters svmJobParameters;

  private String expType = "task"; //task or tset

  public ResultsSaver(double trainingTime, double testingTime, double dataLoadingTime,
                      double totalTime, SVMJobParameters svmJobParameters, String expType) {
    this.svmJobParameters = svmJobParameters;
    this.trainingTime = trainingTime;
    this.testingTime = testingTime;
    this.dataLoadingTime = dataLoadingTime;
    this.totalTime = totalTime;
    this.expType = expType;
  }

  public void save() throws IOException {
    BufferedWriter bufferedWriter = null;
    try {
      File f = new File(this.svmJobParameters.getModelSaveDir());
      String saveStr = "";
      saveStr += this.expType + "," + this.svmJobParameters.getParallelism() + ","
          + this.svmJobParameters.getIterations()
          + "," + this.svmJobParameters.getFeatures()
          + "," + this.svmJobParameters.getSamples() + "," + this.svmJobParameters.getAlpha() + ","
          + this.svmJobParameters.getC() + "," + this.svmJobParameters.getExperimentName() + ", "
          + this.svmJobParameters.getTestingSamples() + "," + this.trainingTime + ", "
          + this.testingTime + "," + this.dataLoadingTime + "," + this.totalTime + ",";
      bufferedWriter = new BufferedWriter(new FileWriter(f, true));
      bufferedWriter.write(saveStr);
      bufferedWriter.newLine();
    } finally {
      bufferedWriter.close();
    }
  }
}
