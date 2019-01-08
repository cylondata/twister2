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
package edu.iu.dsc.tws.examples.batch.cdfw;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class CDFJobParameters {

  private static final Logger LOG = Logger.getLogger(CDFJobParameters.class.getName());

  private int parallelismValue;

  private int workers;

  protected CDFJobParameters(int workers) {
    this.workers = workers;
  }

  /**
   * This method is to build the job parameters which is based on the configuration value.
   */
  public static CDFJobParameters build(Config cfg) {

    int parallelismVal =
        Integer.parseInt(cfg.getStringValue(CDFConstants.ARGS_PARALLELISM_VALUE));
    int workers = Integer.parseInt(cfg.getStringValue(CDFConstants.ARGS_WORKERS));

    CDFJobParameters jobParameters = new CDFJobParameters(workers);
    jobParameters.parallelismValue = parallelismVal;
    jobParameters.workers = workers;

    return jobParameters;
  }

  public int getParallelismValue() {
    return parallelismValue;
  }

  public int getWorkers() {
    return workers;
  }


  @Override
  public String toString() {

    return "JobParameters{"
        + ", workers=" + workers
        + '}';
  }
}

