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
package edu.iu.dsc.tws.rsched.schedulers.k8s.worker;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.squareup.okhttp.Call;
import com.squareup.okhttp.Response;

import edu.iu.dsc.tws.rsched.spi.container.IWorkerLogger;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.ProgressRequestBody;
import io.kubernetes.client.ProgressResponseBody;

public class K8sWorkerLogger extends Thread implements IWorkerLogger {
  private static final Logger LOG = Logger.getLogger(K8sWorkerLogger.class.getName());

  private String logFileName;
  private String namespace;
  private String podName;
  private String containerName;
  private Response response = null;
  private Call call = null;
  private long prevBytesRead = 0;
  private boolean callTerminated = false;
  private FileOutputStream logFileWriter;
  private boolean appendToLogFile = false;
  private boolean firstLogMessageReceived = false;

  // A relative time in seconds before the current time from which to show logs.
  // can be 1 if it is requested to start logging at that moment
  // can be null to start to get log messages from the beginning
  // it gives an error when it is set to zero
  private Integer sinceSeconds = null;

  public K8sWorkerLogger(String logFile, String namespace, String podName, String containerName) {
    this.logFileName = logFile;
    this.namespace = namespace;
    this.podName = podName;
    this.containerName = containerName;
  }

  /**
   * start logging, get the log messages from the beginning time of the worker
   */
  @Override
  public void startLoggingSinceBeginning() {
    sinceSeconds = null;
    start();
  }

  /**
   * start logging, get the log messages starting from now on
   */
  @Override
  public void startLoggingSinceNow() {
    sinceSeconds = 1;
    start();
  }

  /**
   * A relative time in seconds before the current time from which to save logs.
   * sinceSeconds has to be a positive number bigger than 0.
   */
  @Override
  public boolean startLoggingSince(int sinceSecondsValue) {
    if (sinceSecondsValue <= 0) {
      LOG.severe("sinceSeconds has to be a positive number. It is: " + sinceSecondsValue);
      return false;
    } else {
      this.sinceSeconds = sinceSecondsValue;
      start();
      return true;
    }
  }

  /**
   * return true if the first log message is received
   */
  @Override
  public boolean isFirstLogMessageReceived() {
    return firstLogMessageReceived;
  }

  /**
   * if the append is true, append to the existing log file if it exists,
   * if it is false, overwrite the existing log file if it exists
   * by default, it is false
   */
  public void appendToLogFile(boolean append) {
    this.appendToLogFile = append;
  }

  public void setLogFileName(String logFileName) {
    this.logFileName = logFileName;
  }

  @Override
  public String getLogFileName() {
    return logFileName;
  }

  @Override
  public void run() {
    try {
      logFileWriter = new FileOutputStream(logFileName, appendToLogFile);

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when creating the log file: " + logFileName, e);
      return;
    }

    streamContainerLogs();
  }

  /**
   * start log listener
   */
  public boolean streamContainerLogs() {

    ProgressResponseBody.ProgressListener progressListener =
        new ProgressResponseBody.ProgressListener() {
          @Override
          public void update(long bytesRead, long contentLength, boolean done) {

            if (done) {
              LOG.severe("listening for logs done.");
            }

            if (!firstLogMessageReceived) {
              LOG.info("Worker logger started and first log message received. Worker log file: "
                  + logFileName);
              firstLogMessageReceived = true;
            }

            try {
              int receivedBytes = (int) (bytesRead - prevBytesRead);
//                                System.out.println("receivedBytes: " + receivedBytes);
              prevBytesRead = bytesRead;
              byte[] receivedData = new byte[receivedBytes];
              int readBytes = response.body().source().read(receivedData);
              if (readBytes == -1) {
                LOG.severe("The buffer is exhausted. Something wrong.");
              }

              logFileWriter.write(receivedData);
            } catch (IOException e) {
              LOG.log(Level.SEVERE, "Exception when reading the log message from the log stream.",
                  e);
            }
          }
        };

    ProgressRequestBody.ProgressRequestListener progressRequestListener =
        new ProgressRequestBody.ProgressRequestListener() {
          @Override
          public void onRequestProgress(long bytesWritten, long contentLength, boolean done) {
          }
        };

    Boolean followLogStream = true;

    // no limit on received data
    Integer limitBytes = null;

    // could be "true" to get pretty output
    String pretty = "false";

    // get logs for the container that has already terminated
    Boolean previouslyTerminated = false;

    // If set, the number of lines from the end of the logs to show.
    // If not specified, logs are shown from the creation of the container
    Integer tailLines = null;

    // If true, add a Nano timestamp at the beginning of every line of log output.
    Boolean timestamps = false;

    try {
      call = WorkerController.getCoreApi().readNamespacedPodLogCall(
          podName, namespace, containerName, followLogStream, limitBytes, pretty,
          previouslyTerminated, sinceSeconds, tailLines, timestamps,
          progressListener, progressRequestListener);

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when constructing the Call. Exception: " + e, e);
      return false;
    }

    try {
      response = call.execute();

      if (!response.isSuccessful()) {
        LOG.log(Level.SEVERE, "Log Call execute failed. Response code: " + response.code()
            + "Response as string: " + response.toString());
        return false;
      }

      // waits at this point to get all log messages continually
      // when closed, this line throws an IOException that we ignore
      // parameter 10000 does not seem to be important
      response.body().source().request(10000);
      return true;

    } catch (IOException e) {

      if (callTerminated) {
        // no need to do anything with the exception.
        // the call is terminated by the user
        return true;
      }

      LOG.log(Level.SEVERE, "Exception message: " + e.getMessage(), e);
      return false;
    }
  }

  /**
   * stop getting log messages
   */
  @Override
  public void stopLogger() {

    // first close the log file writer
    try {
      logFileWriter.flush();
      logFileWriter.close();
    } catch (IOException e) {
    }

    // terminate the call for the log
    callTerminated = true;
    call.cancel();
  }

}
