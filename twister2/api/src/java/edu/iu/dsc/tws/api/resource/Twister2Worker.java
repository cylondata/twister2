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
package edu.iu.dsc.tws.api.resource;

import java.io.Serializable;

public interface Twister2Worker extends Serializable {

  /**
   * This is the main point of entry for Twister2 jobs. Every job should implement
   * this interface. When a job is submitted, a class implementing this interface
   * gets instantiated and executed by Twister2.
   *
   * As the first thing in the execute method, users are expected to initialize
   * the proper environment object: for batch jobs: BatchTSetEnvironment for
   * streaming jobs: StreamingTSetEnvironment
   */
  void execute(WorkerEnvironment workerEnv);
}
