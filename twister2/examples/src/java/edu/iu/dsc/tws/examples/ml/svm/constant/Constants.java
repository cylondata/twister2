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
package edu.iu.dsc.tws.examples.ml.svm.constant;

public final class Constants {

  private Constants() { }

  public abstract class SimpleGraphConfig {

    public static final String DATA_EDGE = "data-edge";

    public static final String REDUCE_EDGE = "reduce-edge";

    public static final String DATASTREAMER_SOURCE = "datastreamer_source";

    public static final String SVM_COMPUTE = "svm_compute";

    public static final String SVM_REDUCE = "svm_reduce";

    public static final String INPUT_DATA = "input_data";

    public static final String DATA_OBJECT_SOURCE = "data_source";

    public static final String DATA_OBJECT_SINK = "data_sink";

    public static final String INPUT_TESTING_DATA = "input_testing_data";

    public static final String DATA_OBJECT_SOURCE_TESTING = "data_source_test";

    public static final String DATA_OBJECT_SINK_TESTING = "data_sink_test";

    public static final String DELIMITER = ",";

    public static final String PREDICTION_SOURCE_TASK = "prediction_source_task";

    public static final String PREDICTION_REDUCE_TASK = "prediction_reduce_task";

    public static final String PREDICTION_EDGE = "prediction_edge";

    public static final String TEST_DATA = "test_data";

    public static final String FINAL_WEIGHT_VECTOR = "final_weight_vector";

    public static final String CROSS_VALIDATION_DATA = "test_data";

    public static final String SVM_RUN_TYPE = "svm_run_type"; // tset, task, comms, etc

    public static final String TSET_RUNNER = "tset";

    public static final String TASK_RUNNER = "task";


  }
}
