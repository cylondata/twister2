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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.driver.IDriver;
import edu.iu.dsc.tws.common.driver.IDriverController;

public class DriverExample implements IDriver {
  private static final Logger LOG = Logger.getLogger(DriverExample.class.getName());

  @Override
  public void execute(IDriverController driverController) {

    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LOG.info("Will scale up workers by 4");
    driverController.scaleUpWorkers(4);

    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LOG.info("Will scale up workers by 2");
    driverController.scaleUpWorkers(2);

    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LOG.info("Will scale down workers by 4");
    driverController.scaleDownWorkers(4);

    LOG.info("Driver finished execution.");
  }
}
