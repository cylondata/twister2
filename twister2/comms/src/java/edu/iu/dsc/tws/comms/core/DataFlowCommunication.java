//
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
package edu.iu.dsc.tws.comms.core;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.Operation;

public abstract class DataFlowCommunication implements TWSCommunication {
  private static final Logger LOG = Logger.getLogger(DataFlowCommunication.class.getName());

  /**
   * The configuration read from the configuration file
   */
  protected Config config;

  /**
   * Instance plan containing mappings from communication specific ids to higher level task ids
   */
  protected TaskPlan instancePlan;

  public DataFlowOperation setUpDataFlowOperation(Operation operation, int task,
                                                  Set<Integer> sources,
                                                  Set<Integer> destinations,
                                                  Map<String, Object> configuration,
                                                  int stream,
                                                  MessageReceiver receiver,
                                                  MessageDeSerializer formatter,
                                                  MessageSerializer builder) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(configuration).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = create(operation);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, task, instancePlan, sources,
        destinations, stream, receiver, formatter, builder);

    return dataFlowOperation;
  }

  public abstract DataFlowOperation create(Operation operation);

  @Override
  public void init(Config config, TaskPlan taskPlan) {
    this.instancePlan = taskPlan;
    this.config = config;
  }
}
