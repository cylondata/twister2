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
package edu.iu.dsc.tws.common.worker;

import java.io.File;

public interface IPersistentVolume {

  /**
   * get the path of the shared persistent job directory as a string
   * @return
   */
  String getJobDirPath();

  /**
   * get the directory path of this worker as a string
   * @return
   */
  String getWorkerDirPath();

  /**
   * check whether job directory exists
   * @return
   */
  boolean jobDirExists();

  /**
   * check whether worker directory exist
   * @return
   */
  boolean workerDirExists();

  /**
   * get the job directory as a File object
   * @return
   */
  File getJobDir();

  /**
   * get the worker directory as a File object
   * this method creates the directory if it is not already created
   * @return
   */
  File getWorkerDir();

  /**
   * get logfile name for this worker
   * @return
   */
  String getLogFileName();
}
