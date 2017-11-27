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
package edu.iu.dsc.tws.rsched.utils;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class JobUtils {

  private JobUtils() {
  }

  /**
   * Write the job file
   *
   * @param job
   * @param fileName
   * @return
   */
  public static boolean writeJobFile(JobAPI.Job job, String fileName) {
    // lets write a job file
    byte[] jobBytes = job.toByteArray();
    return FileUtils.writeToFile(fileName, jobBytes, true);
  }

  /**
   * Read the job file
   *
   * @param cfg
   * @param fileName
   * @return
   */
  public static JobAPI.Job readJobFile(Config cfg, String fileName) {
    try {
      byte[] fileBytes = FileUtils.readFromFile(fileName);
      JobAPI.Job.Builder builder = JobAPI.Job.newBuilder();

      return builder.mergeFrom(fileBytes).build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to read the job file: " + fileName);
    }
  }

  public static Map<String, Object> readCommandLineOpts() {
    Map<String, Object> ret = new HashMap<>();
    String commandOptions = System.getenv("TWISTER2_OPTIONS");
    if (commandOptions != null) {
      String[] configs = commandOptions.split(",");
      for (String config : configs) {
        String[] options = config.split(":");
        if (options.length == 2) {
          ret.put(options[0], options[1]);
        }
      }
    }
    return ret;
  }

  public static String jobClassPath(JobAPI.Job job) {
    StringBuilder classPathBuilder = new StringBuilder();
    // TODO(nbhagat): Take type of package as argument.
//    if (job.getJobFormat().getType().endsWith(".jar")) {
//      // Bundled jar
//      classPathBuilder.append(originalPackage);
//    } else {
//      // Bundled tar
//      String topologyJar = originalPackage.replace(".tar.gz", "").replace(".tar", "") + ".jar";
//      classPathBuilder.append(String.format("libs/*:%s", topologyJar));
//    }
    return classPathBuilder.toString();
  }

  public static String systemClassPath(Config cfg) {
    return "libs/*";
  }
}
