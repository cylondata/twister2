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
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import java.util.logging.Logger;

import edu.iu.dsc.tws.task.tsystem.TaskInputFiles;
import edu.iu.dsc.tws.task.tsystem.TaskOutputFiles;

/**
 * This class will be used/extended by the task graph to construct the
 * task graph objects.
 */
public abstract class TaskGraphMapper implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(
      TaskGraphMapper.class.getName());

  private final String taskId;

  private Map<String, List<TaskInputFiles>> taskInputFilesMap =
      new HashMap<>();
  private Map<String, List<TaskOutputFiles>> taskOutputFilesMap =
      new HashMap<>();

  protected TaskGraphMapper(String taskId) {
    this.taskId = taskId;
  }

  public abstract void execute();

  /**
   * When an object implementing interface <code>Runnable</code> is used
   * to create a thread, starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * <p>
   * The general contract of the method <code>run</code> is that it may
   * take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    this.execute();
  }

  public TaskGraphMapper addInputData(Map<String, List<TaskInputFiles>> taskInputFiles) {
    try {
      taskInputFiles.entrySet().forEach(stringListEntry -> {
        if (!this.taskInputFilesMap.containsKey(stringListEntry.getKey())) {
          this.taskInputFilesMap.put(stringListEntry.getKey(), new ArrayList<>());
        }
      });
    } catch (Exception ie) {
      ie.printStackTrace();
    }
    return this;
  }

  public Map<String, List<TaskInputFiles>> getInputData() {
    return this.taskInputFilesMap;
  }

  public TaskGraphMapper addInputData(String inputFileName, Object... taskInputObjects) {
    try {
      if (!this.taskInputFilesMap.containsKey(inputFileName)) {
        this.taskInputFilesMap.put(inputFileName, new ArrayList<>());
        //store the taskinput objects
      }
    } catch (Exception ie) {
      ie.printStackTrace();
    }
    return this;
  }

  public TaskGraphMapper addOutputData(Map<String, List<TaskOutputFiles>> taskOutputFiles) {
    try {
      taskOutputFiles.entrySet().forEach(stringListEntry -> {
        if (!this.taskOutputFilesMap.containsKey(stringListEntry.getKey())) {
          this.taskOutputFilesMap.put(stringListEntry.getKey(), new ArrayList<>());
        }
      });
    } catch (Exception ie) {
      ie.printStackTrace();
    }
    return this;
  }

  public TaskGraphMapper addOutputData(String outputFileName, Object... taskOutputObjects) {
    try {
      if (!this.taskOutputFilesMap.containsKey(outputFileName)) {
        this.taskOutputFilesMap.put(outputFileName, new ArrayList<>());
        //store the taskoutput objects
      }
    } catch (Exception ie) {
      ie.printStackTrace();
    }
    return this;
  }

  public Map<String, List<TaskOutputFiles>> getOutputData() {
    return this.taskOutputFilesMap;
  }
}

