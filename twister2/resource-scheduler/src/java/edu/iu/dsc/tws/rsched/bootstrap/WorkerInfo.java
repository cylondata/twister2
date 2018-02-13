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
package edu.iu.dsc.tws.rsched.bootstrap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.logging.Logger;

public class WorkerInfo {
  public static final Logger LOG = Logger.getLogger(WorkerInfo.class.getName());

  private String workerName;
  private int workerID;

  public WorkerInfo(String workerName, int workerID) {
    this.workerName = workerName;
    this.workerID = workerID;
  }

  public String getWorkerName() {
    return workerName;
  }

  public int getWorkerID() {
    return workerID;
  }

  public InetAddress getWorkerIP() {

    String ipStr = workerName.substring(0, workerName.indexOf(":"));
    try {
      InetAddress ip = InetAddress.getByName(ipStr);
      return ip;
    } catch (UnknownHostException e) {
      e.printStackTrace();
      return null;
    }
  }

  public int getWorkerPort() {
    String portStr = workerName.substring(workerName.indexOf(":") + 1);
    return Integer.parseInt(portStr);
  }

  public byte[] getWorkerIDAsBytes() {
    return new Integer(workerID).toString().getBytes();
  }

  public static int getWorkerIDFromBytes(byte[] data) {
    return Integer.parseInt(new String(data));
  }

  /**
   * this is the inverse of getWorkerInfoAsString method
   * @return
   */
  public static WorkerInfo getWorkerInfoFromString(String str) {
    if (str == null || str.length() < 4) {
      return null;
    }

    String workerName = str.substring(0, str.indexOf("="));
    String idStr = str.substring(str.indexOf("=") + 1, str.indexOf(";"));
    return new WorkerInfo(workerName, Integer.parseInt(idStr));
  }

  public String getWorkerInfoAsString() {
    return workerName + "=" + workerID + ";";
  }

  /**
   * parse job znode content and set the id of this worker
   * @param str
   */
  public static int getWorkerIDByParsing(String str, String workerName) {
    int workerNameIndex = str.indexOf(workerName);
    int idStartIndex = str.indexOf("=", workerNameIndex) + 1;
    int idEndIndex = str.indexOf(";", idStartIndex);
    String idStr = str.substring(idStartIndex, idEndIndex);
    return Integer.parseInt(idStr);
  }

  public void setWorkerID(int workerID) {
    this.workerID = workerID;
  }



  @Override
  public String toString() {
    return "workerName: " + workerName + " workerID: " + workerID;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof WorkerInfo) {
      WorkerInfo theOther = (WorkerInfo) o;
      if (this.workerID == theOther.workerID && this.workerName.equals(theOther.workerName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {

    return Objects.hash(workerName, workerID);
  }
}


