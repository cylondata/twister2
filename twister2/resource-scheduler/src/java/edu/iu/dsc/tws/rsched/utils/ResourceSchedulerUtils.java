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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ResourceSchedulerUtils {
  private static final Logger LOG = Logger.getLogger(ResourceSchedulerUtils.class.getName());

  private ResourceSchedulerUtils() {
  }

  public static boolean setupWorkingDirectory(
      String jobName,
      String workingDirectory,
      String corePackageName,
      String jobPackageURI,
      boolean isVerbose) {

    String corePackagePath = Paths.get(jobPackageURI, corePackageName).toString();
    String corePackageDestination = Paths.get(workingDirectory,
        jobName, corePackageName).toString();

    // And then delete the downloaded release package
    // now lets copy other files
    String dst = Paths.get(workingDirectory, jobName).toString();
    LOG.info(String.format("Downloading package %s to %s", jobPackageURI, dst));
    if (!FileUtils.copyDirectory(jobPackageURI, dst)) {
      LOG.severe(String.format("Failed to copy the file from "
          + "uploaded place %s to working directory %s", jobPackageURI, dst));
    }

    if (!extractPackage(
        dst, corePackageDestination, true, isVerbose)) {
      LOG.severe(String.format("Failed to extract the core package %s to directory %s",
          corePackagePath, dst));
      return false;
    }

    ProcessUtils.extractPackageWithoutDir(workingDirectory + "/" + jobName
            + "/twister2-job.tar.gz",
        workingDirectory + "/" + jobName, false, false);

    return true;
  }

  public static boolean extractPackage(
      String workingDirectory,
      String packageDestination,
      boolean isDeletePackage,
      boolean isVerbose) {

    // untar the heron core release package in the working directory
    if (!ProcessUtils.extractPackage(
        packageDestination, workingDirectory, isVerbose, false)) {
      LOG.severe("Failed to extract package.");
      return false;
    }

    // remove the core release package
    if (isDeletePackage && !FileUtils.deleteFile(packageDestination)) {
      LOG.warning("Failed to delete the package: " + packageDestination);
    }

    return true;
  }

  public static String getHostIP() {
    String hostIP = ResourceSchedulerUtils.getOutgoingHostIP();

    if (hostIP != null) {
      return hostIP;
    }

    // if the host is not connected to Internet, it returns null
    // get address from localhost
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Exception when getting local host address: ", e);
      return null;
    }
  }

  /**
   * get the IP address of the host machine
   * a machine may have multiple IP addresses
   * we want the IP address that is reachable from outside
   * we don't want 127.xxx
   * implementation is based on the suggestion from:
   * stackoverflow.com/questions/9481865/getting-the-ip-address-of-the-current-machine-using-java
   *
   * this only works if the host is connected to outside Internet
   *
   * @return hostIP address tha is used to communicate with outside world
   */
  public static String getOutgoingHostIP() {

    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("google.com", 80));
      return socket.getLocalAddress().getHostAddress();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not connect to google.com to get localHost IP.", e);
      return null;
    }
  }
}
