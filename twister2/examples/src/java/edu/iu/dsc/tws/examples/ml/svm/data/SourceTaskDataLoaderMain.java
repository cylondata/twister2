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
package edu.iu.dsc.tws.examples.ml.svm.data;

import java.util.HashMap;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.utils.MLDataObjectConstants;
import edu.iu.dsc.tws.data.utils.WorkerConstants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.InvalidArguments;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class SourceTaskDataLoaderMain {
  private static final int DEFAULT_WORKERS = 1;
  private static final int DEFAULT_PARALLELISM = 4;
  private static int workers = 1;
  private static int parallelism = 4;
  private static String dataSource = "";

  public static void main(String[] args) throws InvalidArguments {
    if(args.length == 3 ) {
      workers = Integer.parseInt(args[0]);
      parallelism = Integer.parseInt(args[1]);
      dataSource = args[2];
      Config config = ResourceAllocator.loadConfig(new HashMap<>());
      JobConfig jobConfig = new JobConfig();
      jobConfig.put(WorkerConstants.WORKERS, workers);
      jobConfig.put(WorkerConstants.PARALLELISM, parallelism);
      jobConfig.put(MLDataObjectConstants.TRAINING_DATA_DIR, dataSource);
      Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
      jobBuilder.setJobName("SourceTaskDataLoader");
      jobBuilder.setWorkerClass(SourceTaskDataLoader.class.getName());
      jobBuilder.addComputeResource(2, 512, 1.0, workers);
      jobBuilder.setConfig(jobConfig);

      // now submit the job
      Twister2Submitter.submitJob(jobBuilder.build(), config);
    } else {
      throw new InvalidArguments("Invalid Arguments : <workers> <parallelism> <datafile_path>");
    }

  }
}
