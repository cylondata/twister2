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

import java.util.logging.Level;
import java.util.logging.Logger;

public class ResourceSchedulerUtils {
  private static final Logger LOG = Logger.getLogger(ResourceSchedulerUtils.class.getName());

  public static boolean setupWorkingDirectory(
      String workingDirectory,
      String coreReleasePackageURL,
      String coreReleaseDestination,
      String topologyPackageURL,
      String topologyPackageDestination,
      boolean isVerbose) {
    // if the working directory does not exist, create it.
    if (!FileUtils.isDirectoryExists(workingDirectory)) {
      LOG.fine("The working directory does not exist; creating it.");
      if (!FileUtils.createDirectory(workingDirectory)) {
        LOG.severe("Failed to create directory: " + workingDirectory);
        return false;
      }
    }

    // Curl and extract heron core release package and topology package
    // And then delete the downloaded release package
    boolean ret =
        curlAndExtractPackage(
            workingDirectory, coreReleasePackageURL, coreReleaseDestination, true, isVerbose)
            &&
            curlAndExtractPackage(
                workingDirectory, topologyPackageURL, topologyPackageDestination, true, isVerbose);

    return ret;
  }

  public static boolean curlAndExtractPackage(
      String workingDirectory,
      String packageURI,
      String packageDestination,
      boolean isDeletePackage,
      boolean isVerbose) {
    // curl the package to the working directory and extract it
    LOG.log(Level.FINE, "Fetching package {0}", packageURI);
    LOG.fine("Fetched package can overwrite old one.");
    if (!ProcessUtils.curlPackage(
        packageURI, packageDestination, isVerbose, false)) {
      LOG.severe("Failed to fetch package.");
      return false;
    }

    // untar the heron core release package in the working directory
    LOG.log(Level.FINE, "Extracting the package {0}", packageURI);
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
}
