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
package edu.iu.dsc.tws.rsched.uploaders.scp;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.config.TokenSub;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class ScpContext extends SchedulerContext {
  public static final String UPLOAD_DIRECTORY = "twister2.uploader.directory";
  public static final String DOWNLOAD_DIRECTORY = "twister2.download.directory";

  public static final String TWISTER2_UPLOADER_SCP_OPTIONS =
      "twister2.uploader.scp.command.options";
  public static final String TWISTER2_UPLOADER_SCP_CONNECTION =
      "twister2.uploader.scp.command.connection";
  public static final String TWISTER2_UPLOADER_SSH_OPTIONS =
      "twister2.uploader.ssh.command.options";
  public static final String TWISTER2_UPLOADER_SSH_CONNECTION =
      "twister2.uploader.ssh.command.connection";

  /**
   * Get the directory to upload the file
   * @return full path as a string
   */
  public static final String uploaderJobDirectory(Config cfg) {
    return TokenSub.substitute(cfg, cfg.getStringValue(UPLOAD_DIRECTORY,
        "/root/.twister2/repository/"), Context.substitutions);
  }

  public static String downloadDirectory(Config config) {
    return config.getStringValue(DOWNLOAD_DIRECTORY);
  }

  public static String scpOptions(Config config) {
    return config.getStringValue(TWISTER2_UPLOADER_SCP_OPTIONS);
  }

  public static String scpConnection(Config config) {
    return config.getStringValue(TWISTER2_UPLOADER_SCP_CONNECTION);
  }

  public static String sshOptions(Config config) {
    return config.getStringValue(TWISTER2_UPLOADER_SSH_OPTIONS);
  }

  public static String sshConnection(Config config) {
    return config.getStringValue(TWISTER2_UPLOADER_SSH_CONNECTION);
  }
}
