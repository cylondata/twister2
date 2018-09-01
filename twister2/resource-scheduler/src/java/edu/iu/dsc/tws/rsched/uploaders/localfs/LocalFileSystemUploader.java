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
package edu.iu.dsc.tws.rsched.uploaders.localfs;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.exceptions.UploaderException;
import edu.iu.dsc.tws.rsched.interfaces.IUploader;
import edu.iu.dsc.tws.rsched.utils.FileUtils;

public class LocalFileSystemUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemUploader.class.getName());

  // this is the directory where to upload the file
  private String destinationDirectory;

  @Override
  public void initialize(Config config) {
    this.destinationDirectory = FsContext.uploaderJobDirectory(config);
  }

  @Override
  public URI uploadPackage(String sourceLocation) throws UploaderException {
    // we shouldn't come here naturally as a jar file is needed for us to get here
    File file = new File(sourceLocation);
    boolean fileExists = file.isDirectory();
    if (!fileExists) {
      throw new UploaderException(
          String.format("Job package does not exist at '%s'", sourceLocation));
    }

    String directoryName = file.getName();
    // get the directory containing the file
    Path filePath = Paths.get(destinationDirectory);
    File parentDirectory = filePath.toFile();
    assert parentDirectory != null;

    // if the dest directory does not exist, create it.
    if (!parentDirectory.exists()) {
      LOG.log(Level.INFO, String.format(
          "Working directory does not exist. Creating it now at %s", parentDirectory.getPath()));
      if (!parentDirectory.mkdirs()) {
        throw new UploaderException(
            String.format("Failed to create directory for topology package at %s",
                parentDirectory.getPath()));
      }
    }

    // if the dest file exists, write a log message
    fileExists = new File(filePath.toString()).isFile();
    if (fileExists) {
      LOG.fine(String.format("Target job package already exists at '%s'. Overwriting it now",
          filePath.toString()));
    }

    // copy the topology package to target working directory
    LOG.log(Level.INFO, String.format("Copying job directory at '%s' to target "
        + "working directory '%s'", sourceLocation, filePath.toString()));
    try {
      //if (!FileUtils.copyDirectoryToDirectory(sourceLocation, destinationDirectory)) {//prev. ver
      if (!FileUtils.copyDirectory(sourceLocation, destinationDirectory)) {
        throw new RuntimeException(String.format("Failed to copy directory %s to %s",
            filePath.toString(), destinationDirectory));
      }
      //return new URI(Paths.get(destinationDirectory, directoryName).toString());//previous version
      return new URI(destinationDirectory);
    }  catch (URISyntaxException e) {
      throw new RuntimeException("Invalid file path for topology package destination: "
          + destinationDirectory, e);
    }
  }

  @Override
  public boolean undo() {
    LOG.info("Clean uploaded jar");
    File file = new File(destinationDirectory);
    return file.delete();
  }

  @Override
  public void close() {
  }
}
