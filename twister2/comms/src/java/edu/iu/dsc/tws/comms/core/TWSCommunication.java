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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;

public interface TWSCommunication {
  void init(Config config, TaskPlan taskPlan);

  void progress();

  DataFlowOperation reduce(Map<String, Object> properties, MessageType type, int edge,
                            Set<Integer> sourceTasks, int destTask,
                            MessageReceiver reduceReceiver, MessageReceiver partialReceiver);

  DataFlowOperation broadCast(Map<String, Object> properties, MessageType type, int edge,
                               int sourceTask, Set<Integer> destTasks,
                               MessageReceiver receiver);

  DataFlowOperation direct(Map<String, Object> properties, MessageType type, int edge,
                           Set<Integer> sourceTasks, int destTask,
                           MessageReceiver receiver);

  DataFlowOperation loadBalance(Map<String, Object> properties, MessageType type, int edge,
                                Set<Integer> sourceTasks, Set<Integer> destTasks,
                                MessageReceiver receiver);

  DataFlowOperation keyedReduce(Map<String, Object> properties, MessageType type, int edge,
                                       Set<Integer> sourceTasks, Set<Integer> destTasks,
                                       MessageReceiver receiver, MessageReceiver partial);
}
