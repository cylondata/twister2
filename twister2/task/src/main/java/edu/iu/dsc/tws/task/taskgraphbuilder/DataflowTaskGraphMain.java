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

/***
 * This is the main class which acts as a reference for generating task graph
 * and sending the generated task graph to the task graph parser and scheduler.
 */
public class DataflowTaskGraphMain extends DataflowTaskGraphGenerator {

  /**
   * Constructor for the main method to override the methods in dataflowtaskgraph generator.
   */
  public DataflowTaskGraphMain() {

    DataflowTaskGraphGenerator dataflowTaskGraphGenerator =
        new DataflowTaskGraphGenerator();

    TaskMapper task1 = new TaskMapper("1", 1) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(TaskMapper mapper) {
        System.out.println("I am in execute method 2");
      }

      public void execute(String message) {
        System.out.println("message is:" + message);
      }
    };

    TaskMapper task2 = new TaskMapper("2", 2) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(String message) {
        System.out.println("message is:" + message);
      }

      @Override
      public void execute(TaskMapper mapper) {
        System.out.println("I am in execute method 2");
      }
    };

    TaskMapper task3 = new TaskMapper("3", 4) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(String message) {
        System.out.println("message is:" + message);
      }

      @Override
      public void execute(TaskMapper mapper) {
        System.out.println("I am in execute method 2");
      }
    };

    TaskMapper task4 = new TaskMapper("4", 3) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(String message) {
        System.out.println("message is:" + message);
      }

      @Override
      public void execute(TaskMapper mapper) {
        System.out.println("I am in execute method 2");
      }
    };

    CManager cManager1 = new CManager("he");
    CManager cManager2 = new CManager("hello");
    CManager cManager3 = new CManager("hi");

    /*this.generateTaskGraph (task1)
                .generateTaskGraph (task2, task1)
                .generateTaskGraph (task3, task2, task1);

    this.generateDataflowTaskGraph(task1, task2, cManager1)
                .generateDataflowTaskGraph(task2, task3, cManager2)
                .generateDataflowTaskGraph(task3, task1, cManager3);*/

    this.generateDataflowTaskGraph(task1, task2, new CManager("he"))
        .generateDataflowTaskGraph(task2, task3, new CManager("te"))
        .generateDataflowTaskGraph(task3, task1, new CManager("test"));
  }

  public static void main(String[] args) {
    DataflowTaskGraphGenerator dataflowTaskGraphGenerator =
        new DataflowTaskGraphMain();
    System.out.println("Generated Task Graph Is:" + dataflowTaskGraphGenerator);

    DataflowTaskGraphParser dataflowTaskGraphParser =
        new DataflowTaskGraphParser(dataflowTaskGraphGenerator);
    dataflowTaskGraphParser.dataflowTaskGraphParseAndSchedule();

    /*TaskParser TaskGraphParser = new TaskParser(dataflowTaskGraphGenerator);
    TaskGraphParser.taskGraphParseAndSchedule(0);
    TaskGraphParser.taskGraphParseAndSchedule(1);*/

    TaskGraphParser taskGraphParser = new TaskGraphParser(dataflowTaskGraphGenerator);
    taskGraphParser.taskGraphParseAndSchedule();
  }
}

