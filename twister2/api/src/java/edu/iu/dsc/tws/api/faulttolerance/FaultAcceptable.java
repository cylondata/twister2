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

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;

/**
 * This is an interface to mark the components that accepts a fault. When a fault occurs, the
 * environments will propagate it to the associated components.
 */
public interface FaultAcceptable {
  /**
   * A fault has occurred
   *
   * @param fault the fault
   * @throws Twister2Exception if an exception occurs while processing the fault
   */
  void onFault(Fault fault) throws Twister2Exception;
}
