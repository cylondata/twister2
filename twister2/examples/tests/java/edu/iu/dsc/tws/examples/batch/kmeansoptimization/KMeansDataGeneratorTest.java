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
package edu.iu.dsc.tws.examples.batch.kmeansoptimization;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.dataobjects.DataObjectSink;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansDataGeneratorTest {

  @Test
  public void testUniqueSchedules1() throws IOException {

    Config config = getConfig();
    int numFiles = 1;
    int dsize = 100;
    int csize = 4;
    int dimension = 2;

    String dinputDirectory = "/tmp/testdinput";
    String cinputDirectory = "/tmp/testcinput";
    String fileSys = "local";

    KMeansDataGenerator.generateData("txt", new Path(dinputDirectory),
        numFiles, dsize, 100, dimension, config);
    KMeansDataGenerator.generateData("txt", new Path(cinputDirectory),
        numFiles, csize, 100, dimension, config);

    int parallelismValue = 2;
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    DataObjectSource sourceTask = new DataObjectSource();
    DataObjectSink sinkTask = new DataObjectSink();
    taskGraphBuilder.addSource("source", sourceTask, parallelismValue);
    ComputeConnection computeConnection1 = taskGraphBuilder.addSink("sink", sinkTask,
        parallelismValue);
    computeConnection1.direct("source", "direct", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    LocalTextInputPartitioner localTextInputPartitioner = new
        LocalTextInputPartitioner(new Path(dinputDirectory), parallelismValue);
    DataSource<String, ?> source
        = new DataSource<>(config, localTextInputPartitioner, parallelismValue);

    InputSplit<String> inputSplit;
    for (int i = 0; i < parallelismValue; i++) {
      inputSplit = source.getNextSplit(i);
      Assert.assertNotNull(inputSplit);
    }
  }


  @Test
  public void testUniqueSchedules2() throws IOException {

    Config config = getConfig();

    String dinputDirectory = "hdfs://kannan-Precision-5820-Tower-X-Series:9000/tmp/testdinput";
    String cinputDirectory = "hdfs://kannan-Precision-5820-Tower-X-Series:9000/tmp/testcinput";

    int numFiles = 1;
    int dsize = 100;
    int csize = 4;
    int dimension = 2;

    String fileSys = "hdfs";

    KMeansDataGenerator.generateData("txt", new Path(dinputDirectory),
        numFiles, dsize, 100, dimension, config);
    KMeansDataGenerator.generateData("txt", new Path(cinputDirectory),
        numFiles, csize, 100, dimension, config);

    int parallelismValue = 2;
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    DataObjectSource sourceTask = new DataObjectSource();
    DataObjectSink sinkTask = new DataObjectSink();
    taskGraphBuilder.addSource("source", sourceTask, parallelismValue);
    ComputeConnection computeConnection1 = taskGraphBuilder.addSink("sink", sinkTask,
        parallelismValue);
    computeConnection1.direct("source", "direct", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    LocalTextInputPartitioner localTextInputPartitioner = new
        LocalTextInputPartitioner(new Path(dinputDirectory), parallelismValue);
    DataSource<String, ?> source
        = new DataSource<>(config, localTextInputPartitioner, parallelismValue);

    InputSplit<String> inputSplit;
    for (int i = 0; i < parallelismValue; i++) {
      inputSplit = source.getNextSplit(i);
      Assert.assertNotNull(inputSplit);
    }
  }

  private Config getConfig() {
    String twister2Home = "/home/kannan/twister2/bazel-bin/scripts/package/twister2-0.1.0";
    String configDir = "/home/kannan/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";
    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);
    return Config.newBuilder().putAll(config).build();
  }
}

