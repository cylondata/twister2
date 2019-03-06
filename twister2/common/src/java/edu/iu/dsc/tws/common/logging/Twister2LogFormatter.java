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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * This formatter has the capability to log thread name
 */
public class Twister2LogFormatter extends Formatter {

  private static final String FORMAT = LoggingContext.loggingFormat();

  private final Date dat = new Date();

  private String componentName = "-";

  private HashMap<Long, String> threadIdNameMap = new HashMap<>();

  private String getThreadName(long threadId) {
    return threadIdNameMap.computeIfAbsent(threadId, tid -> {
      for (Thread thread : Thread.getAllStackTraces().keySet()) {
        if (thread.getId() == threadId) {
          return thread.getName();
        }
      }
      return "unnamed-thread"; //won't come here
    });
  }

  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  @Override
  public String format(LogRecord record) {
    dat.setTime(record.getMillis());
    String source;
    if (record.getSourceClassName() != null) {
      source = record.getSourceClassName();
      if (record.getSourceMethodName() != null) {
        source += " " + record.getSourceMethodName();
      }
    } else {
      source = record.getLoggerName();
    }
    String message = formatMessage(record);
    String throwable = "";
    if (record.getThrown() != null) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      pw.println();
      record.getThrown().printStackTrace(pw);
      pw.close();
      throwable = sw.toString();
    }
    return String.format(FORMAT,
        dat,
        source,
        record.getLoggerName(),
        record.getLevel().getLocalizedName(),
        message,
        throwable,
        componentName,
        this.getThreadName(record.getThreadID())
    );
  }
}
