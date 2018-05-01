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
package edu.iu.dsc.tws.tsched.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is just to retrieve the data node location values corresponding to the
 * dataset file name. Later, it will be overcome by the actual values from the
 * data management part.
 */

public class DataLocatorUtils {

  public String datasetName;
  public List<String> datasetList = new ArrayList<String>();

  public DataLocatorUtils(List<String> datasetlist) {
    this.datasetList = datasetlist;
  }

  public DataLocatorUtils(String datasetname) {
    this.datasetName = datasetname;
  }

  //To assign a different hostnames for the datasets...
 /* public List<String> findDataNodes() {
    List<String> dataNodes = new ArrayList<>();
    if ("dataset1.txt".equals(datasetName)) {
      dataNodes.add("datanode1");
      dataNodes.add("datanode2");
    } else if ("dataset2.txt".equals(datasetName)) {
      dataNodes.add("datanode3");
      dataNodes.add("datanode4");
    } else if ("dataset3.txt".equals(datasetName)) {
      dataNodes.add("datanode5");
      dataNodes.add("datanode6");
    } else if ("dataset4.txt".equals(datasetName)) {
      dataNodes.add("datanode7");
      dataNodes.add("datanode8");
    }
    return dataNodes;
  }
*/

  //To assign the same hostname for the datasets...
  public List<String> findDataNodes() {
    List<String> dataNodes = new ArrayList<>();
    if ("dataset1.txt".equals(datasetName)) {
      dataNodes.add("datanode1");
      dataNodes.add("datanode2");
    } else if ("dataset2.txt".equals(datasetName)) {
      dataNodes.add("datanode1");
      dataNodes.add("datanode2");
    } else if ("dataset3.txt".equals(datasetName)) {
      dataNodes.add("datanode1");
      dataNodes.add("datanode2");
    } else if ("dataset4.txt".equals(datasetName)) {
      dataNodes.add("datanode1");
      dataNodes.add("datanode2");
    }
    return dataNodes;
  }
}

