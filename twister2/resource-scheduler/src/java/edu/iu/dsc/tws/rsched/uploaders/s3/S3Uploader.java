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
package edu.iu.dsc.tws.rsched.uploaders.s3;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.IUploader;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public class S3Uploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(S3Uploader.class.getName());

  private Config config;
  private JobAPI.Job job;

  private String localJobPackFile;

  @Override
  public void initialize(Config cnfg, JobAPI.Job jb) {
    this.config = cnfg;
    this.job = jb;
  }

  @Override
  public URI uploadPackage(String sourceLocation) throws UploaderException {
    localJobPackFile = sourceLocation + "/" + SchedulerContext.jobPackageFileName(config);

    String uploaderScript = S3Context.uploaderScript(config);
    String s3File = S3Context.s3BucketName(config) + "/"
        + JobUtils.createJobPackageFileName(job.getJobId());
    long linkExpDur = S3Context.linkExpirationDuration(config);

    String cmd = String.format(uploaderScript + " %s %s %s", localJobPackFile, s3File, linkExpDur);
    LOG.info("cmd for s3 upload: " + cmd);
    String[] fullCmd = {"bash", "-c", cmd};

    Process p = null;
    try {
      p = Runtime.getRuntime().exec(fullCmd);
      p.waitFor();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when executing uploader script: " + uploaderScript, e);
      throw new UploaderException("Exception when executing uploader script: " + uploaderScript, e);
    } catch (InterruptedException e) {
      throw new UploaderException("Exception when waiting uploader script to complete: "
          + uploaderScript, e);
    }

    int exitCode = p.exitValue();

    if (exitCode == 0) {
      String url = readUrlFile();
      LOG.info("Job Package Download URL: " + url);
      try {
        return new URI(url);
      } catch (URISyntaxException e) {
        LOG.log(Level.SEVERE, "Can not generate URI for download link: " + url, e);
        throw new UploaderException("Can not generate URI for download link: " + url, e);
      }

    } else if (exitCode == 11) {
      String failMsg = String.format("the job package: %s can not be uploaded successfully to: %s",
          localJobPackFile, s3File);
      throw new UploaderException(failMsg);
    } else if (exitCode == 12) {
      String failMsg = String.format("the job package can not be presigned: %s", s3File);
      throw new UploaderException(failMsg);
    } else {
      String failMsg = String.format("Some error occurred when uploading job package %s to s3: %s",
          localJobPackFile, s3File);
      throw new UploaderException(failMsg);
    }
  }

  private String readUrlFile() throws UploaderException {
    String urlFile = System.getProperty("user.home") + "/.twister2/downloadURL.txt";
    Path path = new File(urlFile).toPath();
    try {
      List<String> lines = Files.readAllLines(path);
      return lines.get(0);
    } catch (IOException e) {
      throw new UploaderException("Exception when reading URL file: " + urlFile, e);
    }
  }

  @Override
  public boolean undo() {
    return false;
  }

  @Override
  public void close() {

  }
}
