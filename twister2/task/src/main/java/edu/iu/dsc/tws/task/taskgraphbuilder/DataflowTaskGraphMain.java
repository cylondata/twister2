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
 * This is the main method to call the dataflow class for generating the dataflow task graph.
 */
public class DataflowTaskGraphMain extends DataflowTaskGraphGenerator {

  /**
   * Constructor for the main method to override the methods in dataflowtaskgraph generator.
   */
  public DataflowTaskGraphMain() {

    DataflowTaskGraphGenerator dataflowTaskGraphGenerator =
        new DataflowTaskGraphGenerator();

    Mapper task1 = new Mapper("1", 1) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(Mapper mapper) {
        System.out.println("I am in execute method 2");
      }

      public void execute(String message) {
        System.out.println("message is:" + message);
      }
    };

    Mapper task2 = new Mapper("2", 2) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(String message) {
        System.out.println("message is:" + message);
      }

      @Override
      public void execute(Mapper mapper) {
        System.out.println("I am in execute method 2");
      }
    };

    Mapper task3 = new Mapper("3", 4) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(String message) {
        System.out.println("message is:" + message);
      }

      @Override
      public void execute(Mapper mapper) {
        System.out.println("I am in execute method 2");
      }
    };

    Mapper task4 = new Mapper("4", 3) {
      @Override
      public void execute() {
        System.out.println("I am in execute method 1");
      }

      @Override
      public void execute(String message) {
        System.out.println("message is:" + message);
      }

      @Override
      public void execute(Mapper mapper) {
        System.out.println("I am in execute method 2");
      }
    };

    CManager cManager1 = new CManager("he");
    CManager cManager2 = new CManager("hello");
    CManager cManager3 = new CManager("hi");

    /*this.generateTaskGraph (task1)
                .generateTaskGraph (task2, task1)
                .generateTaskGraph (task3, task2, task1);

    this.generateDataflowGraph(task1, task2, cManager1)
                .generateDataflowGraph(task2, task3, cManager2)
                .generateDataflowGraph(task3, task1, cManager3);*/

    //This condition is not working for task1 -> task2
    // cManager1 is assigned as a task edge for other
    //tasks null values are assigned.
    /*this.generateDataflowGraph(task1, task2, cManager1)
                .generateDataflowGraph(task2, task3, cManager1)
                .generateDataflowGraph(task3, task1, cManager1);*/

    this.generateDataflowGraph(task1, task2, new CManager("he"))
        .generateDataflowGraph(task2, task3, new CManager("te"))
        .generateDataflowGraph(task3, task1, new CManager("test"));
  }

  public static void main(String[] args) {
    DataflowTaskGraphGenerator dataflowTaskGraphGenerator = new DataflowTaskGraphMain();
    System.out.println("Generated Task Graph Is:" + dataflowTaskGraphGenerator);

    /*DataflowTaskGraphParser dataflowTaskGraphParser =
        new DataflowTaskGraphParser(dataflowTaskGraphGenerator);
    dataflowTaskGraphParser.taskGraphParseAndSchedule();*/
  }
}

