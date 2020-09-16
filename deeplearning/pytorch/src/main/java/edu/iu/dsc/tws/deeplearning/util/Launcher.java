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
package edu.iu.dsc.tws.deeplearning.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

public final class Launcher {

  private static final Logger LOG = Logger.getLogger(Launcher.class.getName());

  private Launcher() {
  }

  public static void run(int workerId, String scriptPath, String pythonPath) throws IOException {
    String[] cmd = new String[2];
    cmd[0] = pythonPath;
    cmd[1] = scriptPath;

    // create runtime to execute external command
    Runtime rt = Runtime.getRuntime();
    Process pr = rt.exec(cmd);
    // retrieve output from python script
    BufferedReader bfr = new BufferedReader(new InputStreamReader(pr.getInputStream()));
    String line = "";
    while ((line = bfr.readLine()) != null) {
      // display each output line form python script
      LOG.info("From Python => Tws WorkerId:" + workerId + "=" + line);
    }
  }

}
