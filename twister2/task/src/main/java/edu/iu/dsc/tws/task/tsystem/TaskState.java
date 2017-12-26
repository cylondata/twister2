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
package edu.iu.dsc.tws.task.tsystem;

public enum TaskState {

  /**
   * The task has not yet started just launched the task
   **/
  LAUNCH,
  /**
   * The task is in the running status
   **/
  RUNNING,
  /**
   * The task is stopped for some reason
   **/
  STOPPED,
  /**
   * The task is failed for some reason
   **/
  FAILED,
  /**
   * The task is finished successfully
   **/
  FINISHED,
  /**
   * The task is lost due to some reason
   **/
  LOST,
  /**
   * The task is killed manually or some other reason
   **/
  KILLED,
  /**
   * The task went to unknown state
   **/
  UNKNOWN

}
