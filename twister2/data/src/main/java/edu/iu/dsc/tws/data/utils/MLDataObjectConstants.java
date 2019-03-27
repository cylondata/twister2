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

public final class MLDataObjectConstants {

  private MLDataObjectConstants() {
  }

  public static final String STREAMING = "streaming";

  public static final String TRAINING_DATA_DIR = "training_data_dir";

  public static final String TESTING_DATA_DIR = "testing_data_dir";

  public static final String CROSS_VALIDATION_DATA_DIR = "testing_data_dir";

  public static final String MODEL_SAVE_PATH = "model_save_dir";

  public static final String SPLIT = "split";

  public static final String RATIO = "ratio";

  public static final String DUMMY = "dummy";

  public abstract class SgdSvmDataObjectConstants {

    public static final String FEATURES = "features";

    public static final String SAMPLES = "samples";

    public static final String TRAINING_SAMPLES = "training_samples";

    public static final String TESTING_SAMPLES = "testing_samples";

    public static final String ITERATIONS = "iterations";

    public static final String ALPHA = "alpha";

    public static final String C = "C";

    public static final String EXP_NAME = "exp_name";

  }

}
