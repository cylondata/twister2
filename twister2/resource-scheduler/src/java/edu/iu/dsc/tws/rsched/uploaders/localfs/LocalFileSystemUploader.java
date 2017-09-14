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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.spi.uploaders.IUploader;
import edu.iu.dsc.tws.rsched.spi.uploaders.UploaderException;

public class LocalFileSystemUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemUploader.class.getName());

  // this is the place where we will upload the file
  private String destinationFile;
  // this is the directory where to upload the file
  private String destinationDirectory;
  // this is the original file
  private String originalFile;

  @Override
  public void initialize(Config config) {
    this.destinationDirectory = FsContext.getDirectory(config);
    this.originalFile = FsContext.getJobPackageFile(config);
    // create a random file name
    String fileName = FsContext.getJobName(config) + "tar.gz";
    this.destinationFile = Paths.get(destinationDirectory, fileName).toString();
  }

  @Override
  public URI uploadPackage() throws UploaderException {
    // we shouldn't come here naturally as a jar file is needed for us to get here
    boolean fileExists = new File(originalFile).isFile();
    if (!fileExists) {
      throw new UploaderException(
          String.format("Job package does not exist at '%s'", originalFile));
    }

    // get the directory containing the file
    Path filePath = Paths.get(destinationFile);
    File parentDirectory = filePath.getParent().toFile();
    assert parentDirectory != null;

    // if the dest directory does not exist, create it.
    if (!parentDirectory.exists()) {
      LOG.log(Level.FINE, String.format(
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
      LOG.fine(String.format("Target topology package already exists at '%s'. Overwriting it now",
          filePath.toString()));
    }

    // copy the topology package to target working directory
    LOG.log(Level.FINE, String.format("Copying topology package at '%s' to target "
        + "working directory '%s'",  originalFile, filePath.toString()));

    Path source = Paths.get(originalFile);
    try {
      CopyOption[] options = new CopyOption[]{StandardCopyOption.REPLACE_EXISTING};
      Files.copy(source, filePath, options);
      StringBuilder sb = new StringBuilder()
          .append("file://")
          .append(destinationDirectory);
      return new URI(sb.toString());
    } catch (IOException e) {
      throw new UploaderException(
          String.format("Unable to copy topology file from '%s' to '%s'",
              source, filePath), e);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid file path for topology package destination", e);
    }
  }

  @Override
  public boolean undo() {
    LOG.info("Clean uploaded jar");
    File file = new File(destinationFile);
    return file.delete();
  }

  @Override
  public void close() {
  }
}
