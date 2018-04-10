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
package edu.iu.dsc.tws.rsched.spi.container;

public interface IWorkerLogger {

  /**
   * start logging, get the log messages from the beginning time of the worker
   */
  void startLoggingSinceBeginning();

  /**
   * start logging, get the log messages starting from now on
   */
  void startLoggingSinceNow();

  /**
   * A relative time in seconds before the current time from which to save logs.
   * sinceSecondsValue has to be a positive number bigger than 0.
   */
  boolean startLoggingSince(int sinceSecondsValue);

  /**
   * returns true if the logger has received the first message
   * @return
   */
  boolean isFirstLogMessageReceived();

  /**
   * get the log file name
   */
  String getLogFileName();

  /**
   * stop the logger. Stops getting log messages. Does not delete the log file.
   */
  void stopLogger();
}
