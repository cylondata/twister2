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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.task.api.Task;

/**
 * This is the main interface for generating the dataflow task graph.
 */
public interface IDataflowTaskGraphGenerator {

  DataflowTaskGraphGenerator generateDataflowGraph(Task sourceTask,
                                                   Task sinkTask,
                                                   DataFlowOperation... dataFlowOperation);

  DataflowTaskGraphGenerator generateTaskGraph(Task sourceTask,
                                               Task... sinkTask);

  DataflowTaskGraphGenerator generateDataflowTaskGraph(Task taskMapperTask1,
                                                       Task taskMapperTask2,
                                                       CManager... cManagerTask);

  DataflowTaskGraphGenerator generateTaskGraph(TaskMapper sourceTask,
                                               TaskMapper... sinkTask);

  DataflowTaskGraphGenerator generateDataflowTaskGraph(TaskMapper taskMapperTask1,
                                                       TaskMapper taskMapperTask2,
                                                       CManager... cManagerTask);

  DataflowTaskGraphGenerator generateTaskGraph(Task sourceTask,
                                               Task sinkTask,
                                               DataflowOperation... dataFlowOperation);

  DataflowTaskGraphGenerator generateTGraph(TaskGraphMapper sourceTask,
                                            TaskGraphMapper sinkTask,
                                            DataflowOperation... dataflowOperation);

}

