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
package edu.iu.dsc.tws.rsched.schedulers.standalone.bootstrap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.logging.Logger;

import edu.iu.dsc.tws.rsched.utils.FileUtils;
import edu.iu.dsc.tws.rsched.utils.ResourceSchedulerUtils;

import mpi.MPI;
import mpi.MPIException;

public final class MPIBootstrap {

  private static final Logger LOG = Logger.getLogger(MPIBootstrap.class.getName());

  private MPIBootstrap() {
  }

  private static boolean exactFileExists(File file, String md5) throws IOException {
    return file.exists() && FileUtils.md5(file).equals(md5);
  }

  private static boolean allFilesExists(File[] files, String[] md5) throws IOException {
    for (int i = 0; i < files.length; i++) {
      if (!exactFileExists(files[i], md5[i])) {
        return false;
      }
    }
    return true;
  }

  public static void main(String[] args) throws MPIException, IOException {
    LOG.info("Initializing bootstrap procedure...");
    MPI.Init(args);
    int rank = MPI.COMM_WORLD.getRank();

    String jobName = args[0];
    String jobWorkingDirectory = args[1];

    File jobFile = new File(args[2]);
    File coreFile = new File(args[4]);

    LOG.info(String.format("[%d] Starting process of copying %s & %s of %s to %s",
        rank, jobFile.getAbsolutePath(), coreFile.getAbsolutePath(), jobName, jobWorkingDirectory));

    //Determining the resource provider
    File[] srcFiles = {jobFile, coreFile}; //job file, core file
    String[] md5s = {args[3], args[5]};
    boolean[] resourceAvailability = new boolean[MPI.COMM_WORLD.getSize()];
    resourceAvailability[rank] = allFilesExists(srcFiles, md5s);

    LOG.info(String.format("Resource availability of node %d : %b",
        rank, resourceAvailability[rank]));

    MPI.COMM_WORLD.allGather(new boolean[]{resourceAvailability[rank]}, 1, MPI.BOOLEAN,
        resourceAvailability, 1, MPI.BOOLEAN);

    LOG.info("Communication find resource provider completed");

    int resourceProvider = 0;
    boolean allHaveFile = true;
    for (int i = 0; i < resourceAvailability.length; i++) {
      if (resourceAvailability[i]) {
        resourceProvider = i;
      }
      allHaveFile = allHaveFile && resourceAvailability[i];
    }

    //Don't proceed if all are true : All nodes has already have the file
    if (allHaveFile) {
      LOG.info("All members have the file. Exiting bootstrap procedure");
      setupWorkingDirectory(jobName, jobWorkingDirectory, coreFile, jobFile);
      MPI.Finalize();
      return;
    }

    LOG.info(String.format("%d has the resource files. Starting broadcast...", resourceProvider));

    byte[] buff = new byte[0];
    int[] bufferSize = new int[1];

    for (File srcFile : srcFiles) {
      if (rank == resourceProvider) { //read file
        buff = Files.readAllBytes(srcFile.toPath());
        bufferSize[0] = buff.length;
      }

      //exchange file
      LOG.info(String.format("Exchanging buffer size of %s", srcFile.getName()));
      MPI.COMM_WORLD.bcast(bufferSize, 1, MPI.INT, resourceProvider);

      if (rank != resourceProvider) {
        buff = new byte[bufferSize[0]];
      }
      LOG.info(String.format("Exchanging file %s", srcFile.getName()));
      MPI.COMM_WORLD.bcast(buff, bufferSize[0], MPI.BYTE, resourceProvider);

      if (rank != resourceProvider) { //write file
        srcFile.getParentFile().mkdirs();
        Files.write(srcFile.toPath(), buff);
      }
    }

    setupWorkingDirectory(jobName, jobWorkingDirectory, coreFile, jobFile);

    LOG.info("Broadcasting completed");
    MPI.Finalize();
  }

  private static void setupWorkingDirectory(String jobName, String jobWorkingDirectory,
                                            File coreFile, File jobFile) {
    //Setup working directory
    ResourceSchedulerUtils.setupWorkingDirectory(
        jobName,
        jobWorkingDirectory,
        coreFile.getName(),
        jobFile.getParentFile().getAbsolutePath(),
        true
    );
  }
}
