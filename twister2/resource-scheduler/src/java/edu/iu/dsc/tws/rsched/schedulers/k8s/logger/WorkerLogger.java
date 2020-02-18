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
package edu.iu.dsc.tws.rsched.schedulers.k8s.logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import okhttp3.Call;
import okhttp3.Response;

/**
 * save log of a worker or a job master to a log file
 */
public class WorkerLogger extends Thread {
  private static final Logger LOG = Logger.getLogger(WorkerLogger.class.getName());

  private String namespace;
  private String podName;
  private String containerName;
  private String id;
  private String logFileName;
  private String logsDir;
  private CoreV1Api coreV1Api;
  private JobLogger jobLogger;

  // set this flag if logger stopped by stopLogger method
  private boolean stop = false;

  // response for log stream
  private InputStream logStream;
  private Response response;

  public WorkerLogger(String namespace,
                      String podName,
                      String containerName,
                      String id,
                      String logsDir,
                      CoreV1Api coreV1Api,
                      JobLogger jobLogger) {
    this.namespace = namespace;
    this.podName = podName;
    this.containerName = containerName;
    this.id = id;
    this.logsDir = logsDir;
    this.coreV1Api = coreV1Api;
    this.jobLogger = jobLogger;
  }

  public String getID() {
    return id;
  }

  @Override
  public void run() {

    Path logfile = createLogFile();
    LOG.info("Saving " + id + " logs to: " + logFileName);

    try {
      logStream = streamContainerLog();
      Files.copy(logStream, logfile, StandardCopyOption.REPLACE_EXISTING);
      if (!stop) {
        logStream.close();
        response.body().close();
        response.close();
        LOG.info("Logging completed for " + id + " to: " + logFileName);
      }

    } catch (ApiException e) {
      if (!stop) {
        LOG.log(Level.SEVERE, "Cannot get the log stream for " + id, e);
      }
    } catch (IOException e) {
      if (!stop) {
        LOG.log(Level.SEVERE, "Cannot get the log stream for " + id, e);
      }
    }

    jobLogger.workerLoggerCompleted(id);
  }

  /**
   * if the worker is coming from failure,
   * old log files are renamed
   * the index of each existing log file is increased by one
   * current worker log is saved into index 0
   * @return
   */
  private Path createLogFile() {
    logFileName = logsDir + "/" + id + "-0.log";
    Path logfile = Paths.get(logFileName);
    if (Files.notExists(logfile)) {
      return logfile;
    }

    List<Path> existingWorkerLogFiles = getWorkerFiles();

    LOG.fine("Existing log files for worker: ");
    existingWorkerLogFiles.stream().map(f -> f.toString()).forEach(LOG::fine);

    renameOlderLogFiles(existingWorkerLogFiles);

    return logfile;
  }

  private void renameOlderLogFiles(List<Path> logFiles) {
    Map<Integer, Path> filesMap = new TreeMap<>(Collections.reverseOrder());
    for (Path p: logFiles) {
      int lastDashIndex = p.toString().lastIndexOf("-");
      int lastDotIndex = p.toString().lastIndexOf(".");
      int logFileIndex = Integer.parseInt(p.toString().substring(lastDashIndex + 1, lastDotIndex));
      filesMap.put(logFileIndex, p);
    }

    for (Map.Entry<Integer, Path> entry: filesMap.entrySet()) {
      String index = "-" + entry.getKey() + ".";
      String toReplace = "-" + (entry.getKey() + 1) + ".";
      String newLogFile = entry.getValue().toString().replace(index, toReplace);
      try {
        Files.move(entry.getValue(), Paths.get(newLogFile));
        LOG.fine("Log file " + entry.getValue() + " renamed to " + newLogFile);
      } catch (IOException e) {
        LOG.log(Level.WARNING,
            "Cannot rename log file: " + entry.getValue() + " to " + newLogFile, e);
        continue;
      }
    }
  }

  private List<Path> getWorkerFiles() {
    try (Stream<Path> walk = Files.walk(Paths.get(logsDir))) {

      List<Path> workerFiles = walk.filter(Files::isRegularFile)
          .filter(x -> x.toString().contains(id))
          .collect(Collectors.toList());

      return workerFiles;

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when  getting log file list.", e);
      return new LinkedList<>();
    }
  }

  // Important note. You must close this stream or else you can leak connections.
  public InputStream streamContainerLog() throws ApiException, IOException {

    Integer sinceSeconds = null;
    Integer tailLines = null;
    boolean timestamps = false;

    Call call = coreV1Api.readNamespacedPodLogCall(
        podName,
        namespace,
        containerName,
        true,
        null,
        "false",
        false,
        sinceSeconds,
        tailLines,
        timestamps,
        null);
    response = call.execute();
    if (!response.isSuccessful()) {
      throw new ApiException(response.code(), "Logs request failed: " + response.code());
    }
    return response.body().byteStream();
  }


  public void stopLogging() {
    try {
      logStream.close();
    } catch (IOException e) {
    }
    response.body().close();
    response.close();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerLogger that = (WorkerLogger) o;
    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
