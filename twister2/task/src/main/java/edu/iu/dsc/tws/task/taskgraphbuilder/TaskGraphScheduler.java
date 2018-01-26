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
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.Task;

/**
 * This class will be extended further to accommodate various scheduling
 * scenarios and to handle various task graph structure.
 */
public class TaskGraphScheduler {

  private static final Logger LOG = Logger.getLogger(TaskGraphScheduler.class.getName());

  protected Task task;
  protected Map<Task, ArrayList<Integer>> taskMap;

  /**
   * This method receives the set of parsed task graph objects and container id
   * and select the tasks from the set.
   */
  public Map<Task, ArrayList<Integer>> taskgraphScheduler(
      Set<Task> parsedTaskSet, int containerId) {
    if (containerId == 0) {
      task = parsedTaskSet.iterator().next();
    } else if (containerId == 1) {
      int index = 0;
      for (Task processedTask : parsedTaskSet) {
        if (index == 0) {
          ++index;
        } else if (index == 1) {
          ArrayList<Integer> inq = new ArrayList<>();
          inq.add(0);
          task = processedTask;
          taskMap.put(processedTask, inq);
          ++index;
        } else if (index > 2) {
          LOG.info("Task Index is greater than 1");
          break;
        }
      }
    } else if (containerId == 2) {
      int index = 0;
      for (Task processedTask : parsedTaskSet) {
        if (index == 0) {
          ++index;
        } else if (index == 1) {
          ++index;
        } else if (index == 2) {
          ArrayList<Integer> inq1 = new ArrayList<>();
          inq1.add(0);
          task = processedTask;
          taskMap.put(processedTask, inq1);
          ++index;
        } else if (index > 2) {
          LOG.info("Task Index is greater than 2");
          break;
        }
      }
    } else if (containerId == 3) {
      int index = 0;
      for (Task processedTask : parsedTaskSet) {
        if (index == 0) {
          ++index;
        } else if (index == 1) {
          ++index;
        } else if (index == 2) {
          ++index;
        } else if (index == 3) {
          ArrayList<Integer> inq1 = new ArrayList<>();
          inq1.add(1);
          inq1.add(2);
          taskMap.put(processedTask, inq1);
          task = processedTask;
          ++index;
        } else if (index > 3) {
          LOG.info("Task Index is greater than 3");
          break;
        }
      }
    }
    return taskMap;
  }
}
