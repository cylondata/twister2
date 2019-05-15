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
package edu.iu.dsc.tws.api.tset;

import edu.iu.dsc.tws.api.task.ComputeConnection;

/**
 * All classes that are part of the TSet API need to implement this interface if they are
 * included in the execution graph
 */
public interface TBase<T> {

  /**
   * Build this tset
   */
  void build();

  /**
   * method to be called to build self
   * @return
   */
  boolean baseBuild();

  /**
   * Build the connection
   *
   * @param connection connection
   */
  void buildConnection(ComputeConnection connection);
}
