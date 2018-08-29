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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;

public class MesosWorkerLogger {

  public static final Logger LOG = Logger.getLogger(MesosWorkerLogger.class.getName());
  private String logFileName;
  private String logDir;
  private Config cfg;

  public MesosWorkerLogger(Config config, String logDir, String logFileName) {
    this.cfg = config;
    this.logFileName = logFileName;
    this.logDir = logDir;
  }

  /**
   * Initialize the logger
   */
  public void initLogging() {

    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cfg));

    // if persistent logging is requested, initialize it
    if (LoggingContext.persistentLoggingRequested(cfg)) {

      if (LoggingContext.redirectSysOutErr(cfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      LOG.info("logs redirecting to " + logDir + "/" + logFileName);

      LoggingHelper.setupLogging(cfg, logDir, logFileName);

      LOG.info("Persistent logging to file initialized.");
    }
  }

}
