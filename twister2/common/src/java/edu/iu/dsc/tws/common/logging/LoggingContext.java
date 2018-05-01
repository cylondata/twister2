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
package edu.iu.dsc.tws.common.logging;

import edu.iu.dsc.tws.common.config.Config;

public final class LoggingContext {

  private LoggingContext() { }

  public static final String LOGGING_LEVEL_DEFAULT = "INFO";
  public static final String LOGGING_LEVEL = "twister2.logging.level";

  public static final boolean PERSISTENT_LOGGING_REQUESTED_DEFAULT = false;
  public static final String PERSISTENT_LOGGING_REQUESTED = "persistent.logging.requested";

  // max log file size in MB
  public static final int MAX_LOG_FILE_SIZE_DEFAULT = 100;
  public static final String MAX_LOG_FILE_SIZE = "twister2.logging.max.file.size.mb";

  // max log files for a worker
  public static final int MAX_LOG_FILES_DEFAULT = 5;
  public static final String MAX_LOG_FILES = "twister2.logging.maximum.files";

  // whether redirect System.out and System.err to log files
  public static final boolean REDIRECT_SYS_OUT_ERR_DEFAULT = false;
  public static final String REDIRECT_SYS_OUT_ERR = "twister2.logging.redirect.sysouterr";

  public static String loggingLevel(Config cfg) {
    return cfg.getStringValue(LOGGING_LEVEL, LOGGING_LEVEL_DEFAULT);
  }

  public static boolean persistentLoggingRequested(Config cfg) {
    return cfg.getBooleanValue(PERSISTENT_LOGGING_REQUESTED, PERSISTENT_LOGGING_REQUESTED_DEFAULT);
  }

  public static int maxLogFileSize(Config cfg) {
    return cfg.getIntegerValue(MAX_LOG_FILE_SIZE, MAX_LOG_FILE_SIZE_DEFAULT);
  }

  public static int maxLogFileSizeBytes(Config cfg) {
    return cfg.getIntegerValue(MAX_LOG_FILE_SIZE, MAX_LOG_FILE_SIZE_DEFAULT) * 1024 * 1024;
  }

  public static int maxLogFiles(Config cfg) {
    return cfg.getIntegerValue(MAX_LOG_FILES, MAX_LOG_FILES_DEFAULT);
  }

  public static boolean redirectSysOutErr(Config cfg) {
    return cfg.getBooleanValue(REDIRECT_SYS_OUT_ERR, REDIRECT_SYS_OUT_ERR_DEFAULT);
  }
}
