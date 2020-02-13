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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import okhttp3.Call;
import okhttp3.Response;

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

  @Override
  public void run() {

    Path logfile = createLogFile();
    LOG.info("Saving " + id + " logs to: " + logFileName);

    try {
      InputStream logStream = streamContainerLog();
      java.nio.file.Files.copy(logStream, logfile, StandardCopyOption.REPLACE_EXISTING);
      logStream.close();
      LOG.info("Logging completed for " + id + " to: " + logFileName);

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Cannot get the log stream for " + id, e);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Cannot get the log stream for " + id, e);
    }

    jobLogger.workerLoggerCompleted(id);
  }

  /**
   * TODO: if file exist, take care of that case
   * @return
   */
  private Path createLogFile() {
    logFileName = logsDir + "/" + id + ".log";
    Path logfile = Paths.get(logFileName);
//    if (Files.notExists(logfile)) {
//      return logfile;
//    }

    return logfile;
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
    Response response = call.execute();
    if (!response.isSuccessful()) {
      throw new ApiException(response.code(), "Logs request failed: " + response.code());
    }
    return response.body().byteStream();
  }


  public void stopLogging() {

  }

}
