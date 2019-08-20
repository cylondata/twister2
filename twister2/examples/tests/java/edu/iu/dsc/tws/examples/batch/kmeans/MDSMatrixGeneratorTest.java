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
package edu.iu.dsc.tws.examples.batch.kmeans;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.data.api.formatters.BinaryInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.examples.batch.mds.MDSDataObjectSink;
import edu.iu.dsc.tws.examples.batch.mds.MDSDataObjectSource;
import edu.iu.dsc.tws.examples.batch.mds.MatrixGenerator;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;

//import edu.iu.dsc.tws.data.api.formatters.LocalBinaryInputPartitioner;

public class MDSMatrixGeneratorTest {

  @Test
  public void testUniqueSchedules1() {
    String directory = "/tmp/testmatrix";
    String byteType = "big";

    int datasize = 10000;
    int matrixColumLength = 10000;

    Config config = getConfig(datasize);
    MatrixGenerator matrixGen = new MatrixGenerator(config);
    matrixGen.generate(datasize, matrixColumLength, directory, byteType);

    int parallelismValue = 16;
    ComputeGraphBuilder computeGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    computeGraphBuilder.setTaskGraphName("mdsdataprocessing");
    MDSDataObjectSource sourceTask = new MDSDataObjectSource("direct", directory, datasize);
    MDSDataObjectSink sinkTask = new MDSDataObjectSink(matrixColumLength);
    computeGraphBuilder.addSource("source", sourceTask, parallelismValue);
    ComputeConnection computeConnection = computeGraphBuilder.addSink("sink", sinkTask,
        parallelismValue);
    computeConnection.direct("source").viaEdge("direct").withDataType(MessageTypes.OBJECT);
    computeGraphBuilder.setMode(OperationMode.BATCH);

    BinaryInputPartitioner binaryInputPartitioner =
        new BinaryInputPartitioner(new Path(directory), datasize * Short.BYTES);
    DataSource<?, ?> source
        = new DataSource<>(config, binaryInputPartitioner, parallelismValue);

    InputSplit<?> inputSplit;
    for (int i = 0; i < parallelismValue; i++) {
      inputSplit = source.getNextSplit(i);
      Assert.assertNotNull(inputSplit);
    }
  }

  private Config getConfig(int datasize) {
    String twister2Home = "/home/" + System.getProperty("user.dir")
        + "/twister2/bazel-bin/scripts/package/twister2-0.3.0";
    String configDir = "/home/" + System.getProperty("user.dir")
        + "/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";

    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.DSIZE, datasize);
    Config config = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);
    return Config.newBuilder().putAll(config).putAll(jobConfig).build();
  }
}
