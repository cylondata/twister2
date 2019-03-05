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
package edu.iu.dsc.tws.examples.task;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.examples.comms.DataGenerator;
import edu.iu.dsc.tws.examples.comms.JobParameters;
import edu.iu.dsc.tws.examples.verification.ExperimentData;
import edu.iu.dsc.tws.examples.verification.ExperimentVerification;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.IExecution;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public abstract class BenchTaskWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(BenchTaskWorker.class.getName());

  protected static final String SOURCE = "source";

  protected static final String SINK = "sink";

  protected DataFlowTaskGraph dataFlowTaskGraph;

  protected TaskGraphBuilder taskGraphBuilder;

  protected ExecutionPlan executionPlan;

  protected ComputeConnection computeConnection;

  protected static ExperimentData experimentData;

  protected static JobParameters jobParameters;

  protected static Object inputData;

  @Override
  public void execute() {
    experimentData = new ExperimentData();
    jobParameters = JobParameters.build(config);
    experimentData.setTaskStages(jobParameters.getTaskStages());
    taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    if (jobParameters.isStream()) {
      taskGraphBuilder.setMode(OperationMode.STREAMING);
      experimentData.setOperationMode(OperationMode.STREAMING);
      //streaming application doesn't consider iteration as a looping of the action on the
      //same data set. It's rather producing an streaming of data
      experimentData.setIterations(1);
    } else {
      taskGraphBuilder.setMode(OperationMode.BATCH);
      experimentData.setOperationMode(OperationMode.BATCH);
      experimentData.setIterations(jobParameters.getIterations());
    }
    inputData = generateData();
    experimentData.setInput(inputData);
    buildTaskGraph();
    dataFlowTaskGraph = taskGraphBuilder.build();
    executionPlan = taskExecutor.plan(dataFlowTaskGraph);
    IExecution execution = taskExecutor.iExecute(dataFlowTaskGraph, executionPlan);

    // if streaming lets stop after sometime
    if (jobParameters.isStream()) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(1500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          execution.stop();
        }
      }).start();
    }
    execution.progressExecution();
  }

  public abstract TaskGraphBuilder buildTaskGraph();

  protected static class SourceBatchTask extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public SourceBatchTask() {

    }

    public SourceBatchTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      Object val = inputData;
      int iterations = jobParameters.getIterations();
      while (count <= iterations) {
        if (count == iterations) {
          context.end(this.edge);
        } else if (count < iterations) {
          experimentData.setInput(val);
          if (context.write(this.edge, val)) {
            //
          }
        }
        count++;
      }
    }
  }

  protected static class KeyedSourceBatchTask extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;

    private String edge;

    private int count;

    public KeyedSourceBatchTask() {
    }

    public KeyedSourceBatchTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      Object val = inputData;
      int iterations = jobParameters.getIterations();
      while (count <= iterations) {
        if (count == iterations) {
          context.end(this.edge);
        } else if (count < iterations) {
          experimentData.setInput(val);
          if (context.write(edge, count, val)) {
            //
          }
        }
        count++;
      }
    }
  }

  protected static class SourceStreamTask extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public SourceStreamTask() {

    }

    public SourceStreamTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      Object val = inputData;
      experimentData.setInput(val);
      int iterations = jobParameters.getIterations();
      while (count <= iterations) {
        if (context.write(this.edge, val)) {
          //
        }
        count++;
      }
    }
  }

  protected static class KeyedSourceStreamTask extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;

    private String edge;

    private int count = 0;

    public KeyedSourceStreamTask() {
    }

    public KeyedSourceStreamTask(String edge) {
      this.edge = edge;
    }

    @Override
    public void execute() {
      Object val = inputData;
      int iterations = jobParameters.getIterations();
      while (count <= iterations) {
        if (context.write(edge, count, val)) {
          //
        }
        count++;
      }
    }
  }


  protected static Object generateData() {
    return DataGenerator.generateIntData(jobParameters.getSize());
  }

  protected static Object generateEmpty() {
    return DataGenerator.generateIntData(jobParameters.getSize());
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
