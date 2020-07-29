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
package edu.iu.dsc.tws.local.util;

public class FileConstants {

  public static String getLoggerContent() {
    return "handlers=edu.iu.dsc.tws.common.logging.Twister2FileLogHandler, java.util.logging.ConsoleHandler\n" +
        ".level=INFO\n" +
        "edu.iu.dsc.tws.level=INFO\n" +
        "org.apache.curator.level=WARNING\n" +
        "org.apache.zookeeper.level=WARNING\n" +
        "java.util.logging.ConsoleHandler.level=INFO\n" +
        "java.util.logging.ConsoleHandler.formatter=edu.iu.dsc.tws.common.logging.Twister2LogFormatter\n" +
        "edu.iu.dsc.tws.common.logging.Twister2LogFormatter.format=[%1$tF %1$tT %1$tz] [%4$s] [%7$s] [%8$s] %3$s: %5$s %6$s %n\n" +
        "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.level=ALL\n" +
        "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.limit=50000\n" +
        "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.count=1\n" +
        "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.formatter=edu.iu.dsc.tws.common.logging.Twister2LogFormatter\n" +
        "edu.iu.dsc.tws.common.logging.Twister2FileLogHandler.sysouterr=false\n" +
        "edu.iu.dsc.tws.rsched.utils.JobUtils.level=INFO\n" +
        "edu.iu.dsc.tws.rsched.utils.JobUtils.formatter=edu.iu.dsc.tws.common.logging.Twister2LogFormatter";
  }
}