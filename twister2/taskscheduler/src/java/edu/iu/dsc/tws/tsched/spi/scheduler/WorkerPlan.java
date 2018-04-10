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
package edu.iu.dsc.tws.tsched.spi.scheduler;

import java.util.ArrayList;
import java.util.List;

public class WorkerPlan {
  private List<Worker> workers;

  public WorkerPlan() {
    this.workers = new ArrayList<>();
  }

  public WorkerPlan(List<Worker> workers) {
    this.workers = new ArrayList<>(workers);
  }

  public int getNumberOfWorkers() {
    return workers.size();
  }

  public void addWorker(Worker w) {
    this.workers.add(w);
  }

  public Worker getWorker(int id) {
    for (Worker w : workers) {
      if (w.getId() == id) {
        return w;
      }
    }
    return null;
  }
}
