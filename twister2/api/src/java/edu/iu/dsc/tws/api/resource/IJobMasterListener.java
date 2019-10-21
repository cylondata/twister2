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

public interface IJobMasterListener {

  /**
   * informs when the job master joins the job
   * @param jobMasterAddress
   */
  void jobMasterJoined(String jobMasterAddress);

  /**
   * informs when the job master fails.
   */
  void jobMasterFailed();

  /**
   * informs when the job master rejoins after failure
   * @param jobMasterAddress
   */
  void jobMasterRejoined(String jobMasterAddress);
}
