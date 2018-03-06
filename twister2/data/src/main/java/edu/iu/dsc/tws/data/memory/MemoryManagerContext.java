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
package edu.iu.dsc.tws.data.memory;

import java.nio.charset.Charset;

import edu.iu.dsc.tws.common.config.Context;

/**
 * Created by pulasthi on 1/5/18.
 */
public class MemoryManagerContext extends Context {

  /**
   * Default step size of the Bulk Memory Manager.
   */
  public static final int BULK_MM_STEP_SIZE = 10;

  /**
   * Charset used in the memory manager
   */
  public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  public static final int TL_DATA_BUFF_INIT_CAP = 1024;
  public static final int TL_KEY_BUFF_INIT_CAP = 256;
}
