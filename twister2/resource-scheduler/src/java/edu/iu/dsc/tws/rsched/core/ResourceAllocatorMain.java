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
package edu.iu.dsc.tws.rsched.core;

import java.net.URI;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.rsched.spi.scheduler.ILauncher;
import edu.iu.dsc.tws.rsched.spi.scheduler.LauncherException;
import edu.iu.dsc.tws.rsched.spi.statemanager.IStateManager;
import edu.iu.dsc.tws.rsched.spi.uploaders.IUploader;
import edu.iu.dsc.tws.rsched.spi.uploaders.UploaderException;

/**
 * This is the main class that allocates the resources and starts the processes required
 */
public class ResourceAllocatorMain {
  public static final Logger LOG = Logger.getLogger(ResourceAllocatorMain.class.getName());

  private Config config;

  /**
   * Submit the job to the cluster
   */
  public void submitJob() {
    String statemgrClass = SchedulerContext.stateManagerClass(config);
    IStateManager statemgr;

    String launcherClass = SchedulerContext.launcherClass(config);
    ILauncher launcher;

    String uploaderClass = SchedulerContext.uploaderClass(config);
    IUploader uploader;

    // create an instance of state manager
    try {
      statemgr = ReflectionUtils.newInstance(statemgrClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new JobSubmissionException(
          String.format("Failed to instantiate state manager class '%s'", statemgrClass), e);
    }

    // create an instance of launcher
    try {
      launcher = ReflectionUtils.newInstance(launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // create an instance of uploader
    try {
      uploader = ReflectionUtils.newInstance(uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new UploaderException(
          String.format("Failed to instantiate uploader class '%s'", uploaderClass), e);
    }

    // intialize the state manager
    statemgr.initialize(config);

    // now upload the content of the package
    uploader.initialize(config);
    URI packageURI = uploader.uploadPackage();

    // now launch the launcher
    // Update the runtime config with the packageURI
//      Config runtimeAll = Config.newBuilder()
//          .putAll(runtimeWithoutPackageURI)
//          .put(SchedulerContext.JOB_PACKAGE_URI, packageURI)
//          .build();
//
//      launcher.initialize(config, runtimeAll);
//      launcher.launch()

  }
}
