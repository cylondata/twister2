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
package edu.iu.dsc.tws.comms.api;

public interface ObjectBuilder<D, W> {

  /**
   * Total no of bytes that should be read from buffer to completely read the current object
   */
  int getTotalSize();

  /**
   * Amount of bytes read so far.
   */
  int getCompletedSize();

  /**
   * Get the partially build object
   */
  W getPartialDataHolder();

  /**
   * Set the final object. Setting this signals the engine to finish processing current object
   * and move to the next one
   */
  void setFinalObject(D finalObject);
}
