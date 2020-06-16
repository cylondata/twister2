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
package edu.iu.dsc.tws.api.faulttolerance;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulate a fault occurred in the system.
 * -1 in the worker list shows the job master.
 */
public class Fault {
  /**
   * The fault code
   */
  private int code;

  /**
   * Fault message
   */
  private String message;

  /**
   * The workers with the fault
   */
  private List<Integer> workers = new ArrayList<>();

  public Fault(int worker) {
    this.workers.add(worker);
  }

  public Fault(int code, String message) {
    this.code = code;
    this.message = message;
  }

  public int getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }

  public List<Integer> getWorkers() {
    return workers;
  }
}
