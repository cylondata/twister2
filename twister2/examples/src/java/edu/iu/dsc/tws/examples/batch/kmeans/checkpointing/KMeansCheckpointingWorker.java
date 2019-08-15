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

package edu.iu.dsc.tws.examples.batch.kmeans.checkpointing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.checkpointing.Snapshot;
import edu.iu.dsc.tws.api.comms.packing.types.ObjectPacker;
import edu.iu.dsc.tws.api.comms.packing.types.primitive.IntegerPacker;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.checkpointing.worker.CheckpointingWorkerEnv;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorker;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerParameters;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

/**
 * It is the main class for the K-Means clustering which consists of four main tasks namely
 * generation of datapoints and centroids, partition and read the partitioned data points,
 * read the centroids, and finally perform the distance calculation.
 */
public class KMeansCheckpointingWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(KMeansCheckpointingWorker.class.getName());

  private static final String I_KEY = "iter";
  private static final String CENT_OBJ = "centDataObj";

  /**
   * First, the execute method invokes the generateDataPoints method to generate the datapoints file
   * and centroid file based on the respective filesystem submitted by the user. Next, it invoke
   * the DataObjectSource and DataObjectSink to partition and read the partitioned data points
   * respectively through data points task graph. Then, it calls the DataFileReader to read the
   * centroid values from the filesystem through centroid task graph. Next, the datapoints are
   * stored in DataSet \(0th object\) and centroids are stored in DataSet 1st object\). Finally, it
   * constructs the kmeans task graph to perform the clustering process which computes the distance
   * between the centroids and data points.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void execute(Config config,
                      int workerId,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    ComputeEnvironment taskEnv = ComputeEnvironment.init(config, workerId,
        workerController, persistentVolume, volatileVolume);

    CheckpointingWorkerEnv checkpointingEnv =
        CheckpointingWorkerEnv.newBuilder(config, workerId, workerController)
            .registerVariable(I_KEY, IntegerPacker.getInstance())
            .registerVariable(CENT_OBJ, ObjectPacker.getInstance())
            .build();

    Snapshot snapshot = checkpointingEnv.getSnapshot();

    TaskExecutor taskExecutor = taskEnv.getTaskExecutor();

    LOG.info("Task worker starting: " + workerId + " Current snapshot ver: "
        + snapshot.getVersion());

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(config);
    KMeansWorkerUtils workerUtils = new KMeansWorkerUtils(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dataDirectory = kMeansJobParameters.getDatapointDirectory() + workerId;
    String centroidDirectory = kMeansJobParameters.getCentroidDirectory() + workerId;

    workerUtils.generateDatapoints(dimension, numFiles, dsize, csize, dataDirectory,
        centroidDirectory);

    long startTime = System.currentTimeMillis();

    /* First Graph to partition and read the partitioned data points **/
    ComputeGraph datapointsTaskGraph = KMeansWorker.buildDataPointsTG(dataDirectory, dsize,
        parallelismValue, dimension, config);
    //Get the execution plan for the first task graph
    ExecutionPlan datapointsExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, datapointsExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, datapointsExecutionPlan, "datapointsink");

    DataObject<Object> centroidsDataObject;
    if (!snapshot.checkpointAvailable(CENT_OBJ)) {
      /* Second Graph to read the centroids **/
      ComputeGraph centroidsTaskGraph = KMeansWorker.buildCentroidsTG(centroidDirectory, csize,
          parallelismValue, dimension, config);
      //Get the execution plan for the second task graph
      ExecutionPlan centroidsExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
      //Actual execution for the second taskgraph
      taskExecutor.execute(centroidsTaskGraph, centroidsExecutionPlan);
      //Retrieve the output of the first task graph
      centroidsDataObject = taskExecutor.getOutput(centroidsTaskGraph, centroidsExecutionPlan,
          "centroidsink");
    } else {
      centroidsDataObject = (DataObject<Object>) snapshot.get(CENT_OBJ);
    }

    long endTimeData = System.currentTimeMillis();


    /* Third Graph to do the actual calculation **/
    ComputeGraph kmeansTaskGraph = KMeansWorker.buildKMeansTG(parallelismValue, config);

    //Perform the iterations from 0 to 'n' number of iterations
    ExecutionPlan plan = taskExecutor.plan(kmeansTaskGraph);

    int i = (int) snapshot.getOrDefault(I_KEY, 0);      // recover from checkpoint
    for (; i < iterations; i++) {
      //add the datapoints and centroids as input to the kmeanssource task.
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "points", dataPointsObject);
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "centroids", centroidsDataObject);
      //actual execution of the third task graph
      taskExecutor.itrExecute(kmeansTaskGraph, plan);
      //retrieve the new centroid value for the next iterations
      centroidsDataObject = taskExecutor.getOutput(kmeansTaskGraph, plan, "kmeanssink");

      // at each iteration, commit the checkpoint.
      checkpointingEnv.commitSnapshot();
    }
    taskExecutor.waitFor(kmeansTaskGraph, plan);

    DataPartition<?> centroidPartition = centroidsDataObject.getPartition(workerId);
    double[][] centroid = (double[][]) centroidPartition.getConsumer().next();
    long endTime = System.currentTimeMillis();
    if (workerId == 0) {
      LOG.info("Data Load time : " + (endTimeData - startTime) + "\n"
          + "Total Time : " + (endTime - startTime)
          + "Compute Time : " + (endTime - endTimeData));
    }
    LOG.info("Final Centroids After\t" + iterations + "\titerations\t"
        + Arrays.deepToString(centroid));


    taskEnv.close();
  }


  public static void main(String[] args) throws ParseException {
    LOG.info("KMeans Clustering Job with fault tolerance");

    String jobName = "KMeans-faultolerance-job";

    // first load the configurations from command line and config files
    HashMap<String, Object> c = new HashMap<>();
    c.put(Context.JOB_ID, jobName);
    Config config = ResourceAllocator.loadConfig(c);

    Options options = new Options();
    options.addOption(DataObjectConstants.WORKERS, true, "Workers");
    options.addOption(DataObjectConstants.CSIZE, true, "Size of the dapoints file");
    options.addOption(DataObjectConstants.DSIZE, true, "Size of the centroids file");
    options.addOption(DataObjectConstants.NUMBER_OF_FILES, true, "Number of files");
    options.addOption(DataObjectConstants.SHARED_FILE_SYSTEM, false, "Shared file system");
    options.addOption(DataObjectConstants.DIMENSIONS, true, "dim");
    options.addOption(DataObjectConstants.PARALLELISM_VALUE, true, "parallelism");
    options.addOption(DataObjectConstants.ARGS_ITERATIONS, true, "iter");

    options.addOption(Utils.createOption(DataObjectConstants.DINPUT_DIRECTORY,
        true, "Data points Input directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.CINPUT_DIRECTORY,
        true, "Centroids Input directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.OUTPUT_DIRECTORY,
        true, "Output directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.FILE_SYSTEM,
        true, "file system", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));
    int csize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.CSIZE));
    int numFiles = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.NUMBER_OF_FILES));
    int dimension = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DIMENSIONS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.PARALLELISM_VALUE));
    int iterations = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.ARGS_ITERATIONS));

    String dataDirectory = cmd.getOptionValue(DataObjectConstants.DINPUT_DIRECTORY);
    String centroidDirectory = cmd.getOptionValue(DataObjectConstants.CINPUT_DIRECTORY);
    String outputDirectory = cmd.getOptionValue(DataObjectConstants.OUTPUT_DIRECTORY);
    String fileSystem = cmd.getOptionValue(DataObjectConstants.FILE_SYSTEM);

    boolean shared =
        Boolean.parseBoolean(cmd.getOptionValue(DataObjectConstants.SHARED_FILE_SYSTEM));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    jobConfig.put(DataObjectConstants.DINPUT_DIRECTORY, dataDirectory);
    jobConfig.put(DataObjectConstants.CINPUT_DIRECTORY, centroidDirectory);
    jobConfig.put(DataObjectConstants.OUTPUT_DIRECTORY, outputDirectory);
    jobConfig.put(DataObjectConstants.FILE_SYSTEM, fileSystem);
    jobConfig.put(DataObjectConstants.DSIZE, Integer.toString(dsize));
    jobConfig.put(DataObjectConstants.CSIZE, Integer.toString(csize));
    jobConfig.put(DataObjectConstants.WORKERS, Integer.toString(workers));
    jobConfig.put(DataObjectConstants.NUMBER_OF_FILES, Integer.toString(numFiles));
    jobConfig.put(DataObjectConstants.DIMENSIONS, Integer.toString(dimension));
    jobConfig.put(DataObjectConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));
    jobConfig.put(DataObjectConstants.SHARED_FILE_SYSTEM, shared);
    jobConfig.put(DataObjectConstants.ARGS_ITERATIONS, Integer.toString(iterations));

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName(jobName);
    jobBuilder.setWorkerClass(KMeansCheckpointingWorker.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

}

