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

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.data.api.splits.TextInputSplit;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.constant.IterativeSVMConstants;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMDataObjectCompute;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMDataObjectDirectSink;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMWeightVectorObjectCompute;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMWeightVectorObjectDirectSink;
import edu.iu.dsc.tws.examples.ml.svm.data.SVMDataObjectSource;
import edu.iu.dsc.tws.task.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;

public final class TGUtils {

  private static final String DELIMITER = ",";

  private TGUtils() {

  }

  public static ComputeGraph generateGenericDataPointLoader(int samples, int parallelism,
                                                            int numOfFeatures,
                                                            String dataSourcePathStr,
                                                            String dataObjectSourceStr,
                                                            String dataObjectComputeStr,
                                                            String dataObjectSinkStr,
                                                            String graphName,
                                                            Config config,
                                                            OperationMode opMode) {
    SVMDataObjectSource<String, TextInputSplit> sourceTask
        = new SVMDataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataSourcePathStr);
    IterativeSVMDataObjectCompute dataObjectCompute
        = new IterativeSVMDataObjectCompute(Context.TWISTER2_DIRECT_EDGE, parallelism,
        samples, numOfFeatures, DELIMITER);
    IterativeSVMDataObjectDirectSink iterativeSVMPrimaryDataObjectDirectSink
        = new IterativeSVMDataObjectDirectSink();
    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    datapointsComputeGraphBuilder.addSource(dataObjectSourceStr,
        sourceTask,
        parallelism);
    ComputeConnection datapointComputeConnection
        = datapointsComputeGraphBuilder.addCompute(dataObjectComputeStr,
        dataObjectCompute, parallelism);
    ComputeConnection computeConnectionSink = datapointsComputeGraphBuilder
        .addSink(dataObjectSinkStr,
            iterativeSVMPrimaryDataObjectDirectSink,
            parallelism);
    datapointComputeConnection.direct(dataObjectSourceStr)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    computeConnectionSink.direct(dataObjectComputeStr)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsComputeGraphBuilder.setMode(opMode);

    datapointsComputeGraphBuilder.setTaskGraphName(graphName);
    //Build the first taskgraph
    return datapointsComputeGraphBuilder.build();
  }

  public static ComputeGraph buildTrainingDataPointsTG(int dataStreamerParallelism,
                                                       SVMJobParameters svmJobParameters,
                                                       Config config, OperationMode opMode) {
    return generateGenericDataPointLoader(svmJobParameters.getSamples(),
        dataStreamerParallelism, svmJobParameters.getFeatures(),
        svmJobParameters.getTrainingDataDir(),
        Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE,
        Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE,
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK,
        IterativeSVMConstants.TRAINING_DATA_LOADING_TASK_GRAPH,
        config, opMode);

  }

  public static ComputeGraph buildTestingDataPointsTG(int dataStreamerParallelism,
                                                      SVMJobParameters svmJobParameters,
                                                      Config config, OperationMode opMode) {
    return generateGenericDataPointLoader(svmJobParameters.getTestingSamples(),
        dataStreamerParallelism, svmJobParameters.getFeatures(),
        svmJobParameters.getTestingDataDir(),
        Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING,
        Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE_TESTING,
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING,
        IterativeSVMConstants.TESTING_DATA_LOADING_TASK_GRAPH,
        config, opMode);

  }

  public static ComputeGraph buildWeightVectorTG(Config config, int dataStreamerParallelism,
                                                 SVMJobParameters svmJobParameters,
                                                 OperationMode opMode) {
    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE,
        svmJobParameters.getWeightVectorDataDir());
    IterativeSVMWeightVectorObjectCompute weightVectorObjectCompute
        = new IterativeSVMWeightVectorObjectCompute(Context.TWISTER2_DIRECT_EDGE, 1,
        svmJobParameters.getFeatures());
    IterativeSVMWeightVectorObjectDirectSink weightVectorObjectSink
        = new IterativeSVMWeightVectorObjectDirectSink();
    ComputeGraphBuilder weightVectorComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    weightVectorComputeGraphBuilder
        .addSource(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SOURCE,
            dataFileReplicatedReadSource, dataStreamerParallelism);
    ComputeConnection weightVectorComputeConnection = weightVectorComputeGraphBuilder
        .addCompute(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_COMPUTE,
            weightVectorObjectCompute, dataStreamerParallelism);
    ComputeConnection weightVectorSinkConnection = weightVectorComputeGraphBuilder
        .addSink(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK, weightVectorObjectSink,
            dataStreamerParallelism);

    weightVectorComputeConnection.direct(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SOURCE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    weightVectorSinkConnection.direct(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_COMPUTE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.DOUBLE_ARRAY);
    weightVectorComputeGraphBuilder.setMode(opMode);
    weightVectorComputeGraphBuilder
        .setTaskGraphName(IterativeSVMConstants.WEIGHT_VECTOR_LOADING_TASK_GRAPH);

    return weightVectorComputeGraphBuilder.build();
  }


}
