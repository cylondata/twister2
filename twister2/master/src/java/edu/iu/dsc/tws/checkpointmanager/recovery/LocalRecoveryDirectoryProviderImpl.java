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
package edu.iu.dsc.tws.checkpointmanager.recovery;

import java.io.File;
import java.util.logging.Logger;

import javax.print.attribute.standard.JobName;

public class LocalRecoveryDirectoryProviderImpl implements LocalRecoveryDirectoryProvider {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(
      LocalRecoveryDirectoryProviderImpl.class.getName());

  private final File[] allocationBaseDirs;

  private final JobName jobName;


  public LocalRecoveryDirectoryProviderImpl(
      File[] allocationBaseDirs,
      JobName jobName) {

    this.allocationBaseDirs = allocationBaseDirs;
    this.jobName = jobName;

    for (File allocationBaseDir : allocationBaseDirs) {
      allocationBaseDir.mkdirs();
    }
  }

  @Override
  public File allocationBaseDirectory(long checkpointId) {
    return selectAllocationBaseDirectory(
        (((int) checkpointId) & Integer.MAX_VALUE) % allocationBaseDirs.length);
  }

  @Override
  public File selectAllocationBaseDirectory(int idx) {
    return allocationBaseDirs[idx];
  }

  @Override
  public int allocationBaseDirsCount() {
    return allocationBaseDirs.length;
  }
}
