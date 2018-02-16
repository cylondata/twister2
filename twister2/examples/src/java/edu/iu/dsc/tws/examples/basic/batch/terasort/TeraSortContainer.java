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
package edu.iu.dsc.tws.examples.basic.batch.terasort;

import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.basic.batch.terasort.utils.DataLoader;
import edu.iu.dsc.tws.examples.basic.batch.terasort.utils.Record;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

/**
 * Example that performs tera sort
 */
public class TeraSortContainer implements IContainer {
  private static final Logger LOG = Logger.
      getLogger(TeraSortContainer.class.getName());
  private String inputFolder;
  private String filePrefix;
  private String outputFolder;

  private int partitionSampleNodes;
  private int partitionSamplesPerNode;

  private int id;

  private Config config;
  private ResourcePlan resourcePlan;
  private static final int NO_OF_TASKS = 24;

  private int noOfTasksPerExecutor = 2;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {

    this.config = cfg;
    this.id = containerId;
    this.resourcePlan = plan;

    this.noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();
    //Need to get this from the Config
    inputFolder = "/scratch/pulasthi/terasort/input_test";
    outputFolder = "/scratch/pulasthi/terasort/out_test";
    partitionSampleNodes = 12;
    partitionSamplesPerNode = 10000;
    filePrefix = "part";

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();
    for (int i = 0; i < noOfTasksPerExecutor; i++) {
      int taskId = i;
      LOG.info("Local rank: " + i);
      String inputFile = Paths.get(inputFolder, filePrefix
          + id + "_" + Integer.toString(taskId)).toString();
      String outputFile = Paths.get(outputFolder, filePrefix + Integer.toString(id)).toString();
      List<Record> records = DataLoader.load(id, inputFile);

    }

  }
}
