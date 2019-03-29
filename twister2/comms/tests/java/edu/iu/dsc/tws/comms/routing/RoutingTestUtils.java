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
package edu.iu.dsc.tws.comms.routing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.TaskPlan;

public final class RoutingTestUtils {
  private RoutingTestUtils() {
  }

  public static Set<Integer> destinations(int workers, int taskPerWorker) {
    Set<Integer> s = new HashSet<>();
    for (int i = 1; i <= workers * taskPerWorker; i++) {
      s.add(i);
    }
    return s;
  }

  public static TaskPlan createTaskPlan(int workers, int taskPerWorker, int thisExec) {
    Map<Integer, Set<Integer>> execs = new HashMap<>();
    int taskIndex = 0;
    for (int i = 0; i < workers; i++) {
      Set<Integer> tasks = new HashSet<>();
      for (int j = 0; j < taskPerWorker; j++) {
        tasks.add(taskIndex++);
      }
      if (i == 0) {
        tasks.add(workers * taskPerWorker);
      }
      execs.put(i, tasks);
    }

    Map<Integer, Set<Integer>> groups = new HashMap<>();
    for (int i = 0; i < workers; i++) {
      Set<Integer> e = new HashSet<>();
      e.add(i);
      groups.put(i, e);
    }
    return new TaskPlan(execs, groups, thisExec);
  }
}
