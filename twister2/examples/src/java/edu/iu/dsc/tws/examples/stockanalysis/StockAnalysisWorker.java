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
package edu.iu.dsc.tws.examples.stockanalysis;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;


public class StockAnalysisWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(StockAnalysisWorker.class.getName());

  @Override
  public void execute() {
    StockAnalysisWorkerParameters stockAnalysisWorkerParameters
        = StockAnalysisWorkerParameters.build(config);

    int parallel = stockAnalysisWorkerParameters.getParallelismValue();
    String distanceMatrixDirectory = stockAnalysisWorkerParameters.getDataInput();
    String configFile = stockAnalysisWorkerParameters.getConfigFile();
    String directory = stockAnalysisWorkerParameters.getDatapointDirectory();
    String byteType = stockAnalysisWorkerParameters.getByteType();

    String datainputFile = stockAnalysisWorkerParameters.getDinputFile();
    String vectorDirectory = stockAnalysisWorkerParameters.getOutputDirectory();
    String numberOfDays = stockAnalysisWorkerParameters.getNumberOfDays();
    String startDate = stockAnalysisWorkerParameters.getStartDate();
    String endDate = stockAnalysisWorkerParameters.getEndDate();
    String mode = stockAnalysisWorkerParameters.getMode();
    String distanceType = stockAnalysisWorkerParameters.getDistanceType();

    LOG.info("Data Points to be generated or read," + distanceMatrixDirectory + "\t"
        + directory + "\t" + byteType + "\t" + configFile);

    /** Task Graph to do the preprocessing **/
    long startTime = System.currentTimeMillis();
    DataPreProcessingSourceTask preprocessingSourceTask = new DataPreProcessingSourceTask(
        datainputFile, vectorDirectory, numberOfDays, startDate, endDate, mode);
    DataPreprocessingSinkTask preprocessingSinkTask = new DataPreprocessingSinkTask(
        vectorDirectory, distanceMatrixDirectory, distanceType);
    TaskGraphBuilder preprocessingTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    preprocessingTaskGraphBuilder.setTaskGraphName("StockAnalysisDataPreProcessing");
    preprocessingTaskGraphBuilder.addSource("preprocessingsourcetask",
        preprocessingSourceTask, parallel);

    ComputeConnection preprocessingComputeConnection = preprocessingTaskGraphBuilder.addSink(
        "preprocessingsinktask", preprocessingSinkTask, parallel);
    preprocessingComputeConnection.direct("preprocessingsourcetask")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    preprocessingTaskGraphBuilder.setMode(OperationMode.STREAMING);
    DataFlowTaskGraph preprocesingTaskGraph = preprocessingTaskGraphBuilder.build();

    //Get the execution plan for the first task graph
    ExecutionPlan preprocessExecutionPlan = taskExecutor.plan(preprocesingTaskGraph);

    //Actual execution for the first taskgraph
    taskExecutor.execute(preprocesingTaskGraph, preprocessExecutionPlan);

    long endTime = System.currentTimeMillis();
    LOG.info("Compute Time : " + (endTime - startTime));

    /** Task Graph to run the MDS **/
    StockAnalysisSourceTask sourceTask = new StockAnalysisSourceTask();
    StockAnalysisSinkTask sinkTask = new StockAnalysisSinkTask();
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    taskGraphBuilder.setTaskGraphName("StockAnalysisComputeProcessing");
    taskGraphBuilder.addSource("sourcetask", sourceTask, parallel);

    ComputeConnection dataObjectComputeConnection = taskGraphBuilder.addSink("sinktask",
        sinkTask, parallel);
    dataObjectComputeConnection.direct("sourcetask")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph computeTaskGraph = taskGraphBuilder.build();

    //Get the execution plan for the first task graph

    ExecutionPlan computeExecutionPlan = taskExecutor.plan(computeTaskGraph);

    //Actual execution for the first taskgraph
    taskExecutor.execute(computeTaskGraph, computeExecutionPlan);
  }

  private static class StockAnalysisSourceTask extends BaseSource implements Receptor {

    @Override
    public void add(String name, DataObject<?> data) {
    }

    @Override
    public void execute() {
      LOG.info("I am executing the task");
      context.write(Context.TWISTER2_DIRECT_EDGE, "Stock Analysis Execution");
    }
  }

  private static class StockAnalysisSinkTask extends BaseSink implements Collector {

    @Override
    public DataPartition<?> get() {
      return null;
    }

    @Override
    public boolean execute(IMessage content) {
      LOG.info("Received message:" + content.getContent().toString());
      return false;
    }
  }
}
