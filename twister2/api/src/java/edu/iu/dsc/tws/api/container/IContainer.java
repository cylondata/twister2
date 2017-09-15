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
package edu.iu.dsc.tws.api.container;

import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

/**
 * This is the base API according to which a program can be built.
 * A dataflow or MPI style program starting as a container should manage threads, communications and
 * distributed data by its own.
 */
public interface IContainer {
  void init(int containerId, ResourcePlan resourcePlan);
}
