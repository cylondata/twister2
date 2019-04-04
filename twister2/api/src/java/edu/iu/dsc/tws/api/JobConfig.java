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
package edu.iu.dsc.tws.api;

import java.util.HashMap;

/**
 * These are the set of configuration parameters that will be transferred to Worker processes
 * For now, we put all config parameters to this list
 * But later on, we may only put the parameters that are relevant to Worker processes
 */
public class JobConfig extends HashMap<String, Object> {
  private static final long serialVersionUID = 6585146860991205058L;

  //todo refactor tws.api such that it doesn't depend on other packages.
  // Till that keeping below properties(unused) here
  /**
   * Twister2 has the support to use disk for some operations. For example,
   * keyed gather in batch mode can disk to handle tera bytes of data. This flag can be used
   * to signal engine to use disk whenever possible. Please note that, this option is
   * not applicable for all the operations and not intended to use directly.
   */
  public static final String USE_DISK = "use-disk";

  /**
   * Twister2 keyed operations sometimes requires to compare keys. This property
   * can be set to define key comparator for such operations.
   */
  public static final String KEY_COMPARATOR = "key-comparator";
}
