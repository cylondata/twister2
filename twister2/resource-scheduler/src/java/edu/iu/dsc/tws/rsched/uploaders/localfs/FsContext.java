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
package edu.iu.dsc.tws.rsched.uploaders.localfs;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.config.TokenSub;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class FsContext extends SchedulerContext {
  public static final String UPLOAD_DIRECTORY = "twister2.uploader.directory";


  /**
   * Get the directory to upload the file
   * @return full path as a string
   */
  public static final String uploaderJobDirectory(Config cfg) {
    return TokenSub.substitute(cfg, cfg.getStringValue(UPLOAD_DIRECTORY,
        "${HOME}/.twister2/repository"), Context.substitutions);
  }
}
