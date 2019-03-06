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

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import edu.iu.dsc.tws.common.config.Config;

/**
 * This class is a wrapper class for FileHandler.
 * It just acts as a lazy initializer for FileHandler.
 * Lazy init is done to prevent creating log files in the %u directory, which conflicts
 * in SharedFS
 */
public class Twister2FileLogHandler extends Handler {

  public static final String LIMIT = "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.limit";
  public static final String LEVEL = "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.level";
  public static final String COUNT = "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.count";
  public static final String APPEND = "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.append";
  public static final String ENCODING =
      "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.encoding";
  public static final String FORMATTER =
      "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.formatter";
  public static final String REDIRECT_SYS =
      "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.sysouterr";

  private FileHandler fileHandler;

  public void init(String loggingDir, String workerId, Config config) throws IOException {
    String pattern = loggingDir + "/" + workerId + ".log.%g";

    int maxFileSize = LoggingContext.maxLogFileSizeBytes(config);

    // get the value of twister2.logging.maximum.files
    int maxFiles = LoggingContext.maxLogFiles(config);

    boolean appendToFile = LoggingContext.appendToFile();
    this.fileHandler = new FileHandler(
        pattern, maxFileSize, maxFiles, appendToFile
    );
    this.fileHandler.setFormatter(LoggingContext.getFileLogFormatter());
    this.fileHandler.setEncoding(LoggingContext.getFileEncoding());
  }

  @Override
  public void publish(LogRecord record) {
    if (fileHandler != null) {
      fileHandler.publish(record);
    }
  }

  @Override
  public void flush() {
    if (fileHandler != null) {
      fileHandler.flush();
    }
  }

  @Override
  public void close() throws SecurityException {
    if (fileHandler != null) {
      fileHandler.close();
    }
  }
}
