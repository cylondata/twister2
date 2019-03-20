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
package edu.iu.dsc.tws.data.utils;

/**
 * The input parameters required to create, partition, and read the input data points.
 */

public class DataObjectConstants {

  protected DataObjectConstants() {
  }

  public static final String WORKERS = "workers";
  public static final String DIMENSIONS = "dim";
  public static final String PARALLELISM_VALUE = "parallelism";
  public static final String SHARED_FILE_SYSTEM = "fShared";
  public static final String DSIZE = "dsize";
  public static final String CSIZE = "csize";
  public static final String DINPUT_DIRECTORY = "dinput";
  public static final String CINPUT_DIRECTORY = "cinput";
  public static final String OUTPUT_DIRECTORY = "output";
  public static final String NUMBER_OF_FILES = "nFiles";
  public static final String FILE_SYSTEM = "filesys";
  public static final String ARGS_ITERATIONS = "iter";
}
