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
package edu.iu.dsc.tws.examples.basic;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BasicAuroraContainer implements IContainer {


  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {
    // wait some random amount of time before finishing
    long duration = (long) (Math.random() * 1000);
    try {
      System.out.println("I am the worker: " + id);
      System.out.println("I am sleeping " + duration + "ms. Then will close.");
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
