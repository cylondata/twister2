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
package edu.iu.dsc.tws.examples.tset;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.dataobjects.DataObjectConstants;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetBaseWorker;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.examples.batch.kmeansoptimization.KMeansDataGenerator;
import edu.iu.dsc.tws.examples.batch.kmeansoptimization.KMeansJobParameters;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansTsetEnvJob extends TSetBaseWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(KMeansTsetEnvJob.class.getName());

  @Override
  public void execute(TSetEnv executionEnv) {
    LOG.log(Level.INFO, "TSet worker starting: " + workerId);

    KMeansJobParameters kMeansJobParameters = KMeansJobParameters.build(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dinputDirectory = kMeansJobParameters.getDatapointDirectory();
    String cinputDirectory = kMeansJobParameters.getCentroidDirectory();

    if (workerId == 0) {
      try {
        KMeansDataGenerator.generateData("txt", new Path(dinputDirectory), numFiles, dsize,
            100, dimension, executionEnv.getConfig());
        KMeansDataGenerator.generateData("txt", new Path(cinputDirectory), numFiles, csize,
            100, dimension, executionEnv.getConfig());
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create input data:", ioe);
      }
    }
//    TSetBuilder builder = TSetBuilder.newBuilder(config);
    executionEnv.setMode(OperationMode.BATCH);
    TSet<double[][]> points = executionEnv.createSource(new PointsSource()).cache();
//    TSet<double[][]> centers = executionEnv.createSource(new CenterSource()).cache();

    executionEnv.run();


//    for (int i = 0; i < iterations; i++) {
////      TSet<double[][]> KmeansTSet = points.map( )
//    }
  }

  public class PointsSource implements Source<double[][]> {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public double[][] next() {
      return new double[0][];
    }
  }


  public class CenterSource implements Source<double[][]> {

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public double[][] next() {
      return new double[0][];
    }
  }

  public static void main(String[] args) {
    // first load the configMap from command line and config files

    File dir = new File("/tmp/kmeanstset/");
    dir.mkdirs();

    Map<String, Object> configMap = new HashMap<>();

    configMap.put(DataObjectConstants.ARGS_DINPUT_DIRECTORY, dir.getAbsolutePath() + "/data");
    configMap.put(DataObjectConstants.ARGS_CINPUT_DIRECTORY, dir.getAbsolutePath() + "/cent");
    configMap.put(DataObjectConstants.ARGS_OUTPUT_DIRECTORY, "/output");
    configMap.put(DataObjectConstants.ARGS_FILE_SYSTEM, "local");
    configMap.put(DataObjectConstants.ARGS_DSIZE, Integer.toString(100));
    configMap.put(DataObjectConstants.ARGS_CSIZE, Integer.toString(10));
    configMap.put(DataObjectConstants.ARGS_WORKERS, Integer.toString(2));
    configMap.put(DataObjectConstants.ARGS_NUMBER_OF_FILES, Integer.toString(4));
    configMap.put(DataObjectConstants.ARGS_DIMENSIONS, Integer.toString(2));
    configMap.put(DataObjectConstants.ARGS_PARALLELISM_VALUE, Integer.toString(1));
    configMap.put(DataObjectConstants.ARGS_SHARED_FILE_SYSTEM, false);
    configMap.put(DataObjectConstants.ARGS_NUMBER_OF_CLUSTERS, Integer.toString(1));
    configMap.put(DataObjectConstants.ARGS_ITERATIONS, Integer.toString(5));

    // build configMap
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configMap);

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(KMeansTsetEnvJob.class.getName())
        .setWorkerClass(KMeansTsetEnvJob.class.getName())
        .addComputeResource(1, 512, 2)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, ResourceAllocator.loadConfig(new HashMap<>()));
  }
}

