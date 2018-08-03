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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.config.TokenSub;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class StandaloneContext extends SchedulerContext {
  // these are environment variables set in the shell script
  public static final String WORKING_DIRECTORY_ENV = "WORKING_DIRECTORY_ENV";
  public static final String DOWNLOAD_PACKAGE_ENV = "DOWNLOAD_PACKAGE_ENV";
  public static final String CORE_PACKAGE_ENV = "CORE_PACKAGE_ENV";
  public static final String JOB_PACKAGE_ENV = "JOB_PACKAGE_ENV";

  // nomad template values
  public static final String NOMAD_TASK_COMMAND = "command";
  public static final String NOMAD_TASK_COMMAND_ARGS = "args";
  public static final String NOMAD_IMAGE = "image";
  public static final String NOMAD_DEFAULT_DATACENTER = "dc1";
  public static final String SHELL_CMD = "/bin/sh";
  public static final String NOMAD_JOB_NAME = "NOMAD_JOB_NAME";

  // the requested names of ports
  public static final String PORT_NAMES = "twister2.network.port.names";
  // weather we are in a shared file system
  public static final String SHARED_FILE_SYSTEM = "twister2.filesystem.shared";
  // shell script to be executed
  public static final String NOMAD_SHELL_SCRIPT = "twister2.nomad.shell.script";
  public static final String NOMAD_HERON_SCRIPT_NAME = "nomad.sh";
  public static final String NOMAD_URI = "twister2.nomad.scheduler.uri";

  public static final String LOGGING_SANDBOX = "twister2.logging.sandbox.logging";

  public static String workingDirectory(Config config) {
    return TokenSub.substitute(config, config.getStringValue(WORKING_DIRECTORY,
        "${HOME}/.twister2/jobs"), Context.substitutions);
  }

  public static boolean sharedFileSystem(Config config) {
    return config.getBooleanValue(SHARED_FILE_SYSTEM, true);
  }

  public static String networkPortNames(Config config) {
    return config.getStringValue(PORT_NAMES, "worker");
  }

  public static String shellScriptName(Config config) {
    return config.getStringValue(NOMAD_SHELL_SCRIPT, "nomad.sh");
  }

  public static String nomadSchedulerUri(Config config) {
    return config.getStringValue(NOMAD_URI);
  }

  public static boolean getLoggingSandbox(Config config) {
    return config.getBooleanValue(LOGGING_SANDBOX, false);
  }
}
