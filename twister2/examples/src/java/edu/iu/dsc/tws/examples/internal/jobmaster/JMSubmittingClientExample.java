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
package edu.iu.dsc.tws.examples.internal.jobmaster;

import java.nio.file.Paths;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.driver.JMDriverAgent;

public final class JMSubmittingClientExample {

  private JMSubmittingClientExample() { }

  public static void main(String[] args) {
    // we assume that the twister2Home is the current directory
    String configDir = "conf/kubernetes/";
    String twister2Home = Paths.get("").toAbsolutePath().toString();
    Config config = ConfigLoader.loadConfig(twister2Home, configDir);

    String jmHost = "localhost";
    int jmPort = JobMasterContext.jobMasterPort(config);

    JMDriverAgent agent = new JMDriverAgent(config, jmHost, jmPort);
    agent.startThreaded();

    agent.sendScaledMessage(0, 10);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    agent.sendScaledMessage(0, 7);

    agent.close();

  }

}
