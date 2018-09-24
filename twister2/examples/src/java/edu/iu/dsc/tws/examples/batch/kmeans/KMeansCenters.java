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
package edu.iu.dsc.tws.examples.batch.kmeans;

/**
 * This class has the getter and setter properties to get and set the calculated new centroid values
 */
public class KMeansCenters {

  private double[][] centers;
  private int[] centerSums;

  public KMeansCenters() {
  }

  public KMeansCenters(double[][] centerValues) {
    this.centers = centerValues;
  }

  public KMeansCenters(double[][] centerValues, int[] centerSums) {
    this.centers = centerValues;
    this.centerSums = centerSums;
  }

  public double[][] getCenters() {
    return centers;
  }

  public KMeansCenters setCenters(double[][] centerValues) {
    this.centers = centerValues;
    return this;
  }

  public int[] getCenterSums() {
    return centerSums;
  }

  public KMeansCenters setCenterSums(int[] cSums) {
    this.centerSums = cSums;
    return this;
  }
}
