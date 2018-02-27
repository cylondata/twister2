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

import java.util.ArrayList;

public class DataflowTaskGraphBuilder extends DataflowTaskGraphGenerator {

  public DataflowTaskGraphBuilder() {

    DataflowTaskGraphGenerator dataflowTaskGraphGenerator = new DataflowTaskGraphGenerator();

    TaskGraphMapper task1 = new TaskGraphMapper("1") {
      @Override
      public void execute() {
        System.out.println("Task Graph Construction with Input and Output Files");
      }
    };
    task1.addInputData("inputFile1", new ArrayList<>()); //Add the input file list
    task1.addOutputData("outputFile1", new ArrayList<>()); //Add the output file list

    TaskGraphMapper task2 = new TaskGraphMapper("2") {
      @Override
      public void execute() {
        System.out.println("Task Graph Construction with Input and Output Files");
      }
    };
    task2.addInputData("inputFile2", new ArrayList<>()); //Add the input file list
    task2.addOutputData("outputFile2", new ArrayList<>()); //Add the output file list

    //this.generateDataflowGraph()  //Construct the task vertices and edges here...!
    this.generateTGraph(task1, task2, new DataflowOperation("Map"));

  }
}


