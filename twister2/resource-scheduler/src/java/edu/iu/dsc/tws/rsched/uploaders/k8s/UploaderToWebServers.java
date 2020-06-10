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
package edu.iu.dsc.tws.rsched.uploaders.k8s;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public class UploaderToWebServers extends Thread {
  private static final Logger LOG = Logger.getLogger(UploaderToWebServers.class.getName());

  private Config config;
  private String jobID;
  private String localJobPackageFile;
  private String namespace;

  private ArrayList<UploaderToPod> uploadersToWebServers = new ArrayList<>();
  private List<String> webServerPodNames;

  public UploaderToWebServers(Config config, String jobID, List<String> webServerPodNames) {
    this.config = config;
    this.jobID = jobID;
    this.namespace = KubernetesContext.namespace(config);
    this.webServerPodNames = webServerPodNames;
  }

  public URI uploadPackage(String localJobPackage) throws UploaderException {

    localJobPackageFile = localJobPackage;

    // start uploader thread
    start();

    // if this job is a resubmitted job
    // wait for the uploading to web servers to finish
    // job package may exist at the web server and when we are uploading
    // partial files may be served to workers
    if (CheckpointingContext.startingFromACheckpoint(config)) {
      // wait for the uploaders to start
      while (webServerPodNames.size() != uploadersToWebServers.size()) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          // ignore the exception
        }
      }

      LOG.fine("Wait uploading to web servers to finish ...");
      complete();
      LOG.info("Uploading to web servers finished ...");
    }

    String uri = KubernetesContext.uploaderWebServer(config) + "/"
        + JobUtils.createJobPackageFileName(jobID);
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      LOG.log(Level.SEVERE, "Can not generate URI for uploader web server: " + uri, e);
      throw new UploaderException("Can not generate URI for download link: " + uri, e);
    }
  }

  @Override
  public void run() {
    String targetFile = KubernetesUtils.jobPackageFullPath(config, jobID);

    for (String webServerPodName : webServerPodNames) {
      UploaderToPod uploader =
          new UploaderToPod(namespace, webServerPodName, localJobPackageFile, targetFile);
      uploader.start();
      uploadersToWebServers.add(uploader);
    }

  }

  public boolean complete() {
    // wait all transfer threads to finish up
    boolean allTransferred = true;
    for (UploaderToPod uploader: uploadersToWebServers) {

      try {
        uploader.join();
        if (!uploader.packageTransferred()) {
          LOG.log(Level.SEVERE, "Job Package is not transferred to the web server pod: "
              + uploader.getPodName());
          allTransferred = false;
          break;
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }

    // if one transfer fails, tell all transfer threads to stop and return false
    if (!allTransferred) {
      for (UploaderToPod uploader: uploadersToWebServers) {
        uploader.cancelTransfer();
      }
    }

    return allTransferred;
  }

  public boolean undo(Config cnfg, String jbID) {
    String jobPackageFile = KubernetesUtils.jobPackageFullPath(cnfg, jbID);
    return KubernetesController.deleteJobPackage(
        KubernetesContext.namespace(cnfg), webServerPodNames, jobPackageFile);
  }

  public void close() {
  }

}
