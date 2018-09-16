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
package edu.iu.dsc.tws.data.hdfs;

import edu.iu.dsc.tws.data.fs.FileStatus;

public final class HadoopFileStatus implements FileStatus {

  private org.apache.hadoop.fs.FileStatus fileStatus;

  public HadoopFileStatus(org.apache.hadoop.fs.FileStatus fileStatus) {
    this.fileStatus = fileStatus;
  }

  /**
   * To get the length of the filename
   * @return
   */
  @Override
  public long getLen() {
    return fileStatus.getLen();
  }

  /**
   * To get the length of the block size
   * @return
   */
  @Override
  public long getBlockSize() {
    long blocksize = fileStatus.getBlockSize();
    if (blocksize > fileStatus.getLen()) {
      return fileStatus.getLen();
    }

    return blocksize;
  }

  /**
   * To get the access time of the file
   * @return
   */
  @Override
  public long getAccessTime() {
    return fileStatus.getAccessTime();
  }

  /**
   * To get the modification time of the file
   * @return
   */
  @Override
  public long getModificationTime() {
    return fileStatus.getModificationTime();
  }

  /**
   * To get the replication of the file
   * @return
   */
  @Override
  public short getReplication() {
    return fileStatus.getReplication();
  }

  public org.apache.hadoop.fs.FileStatus getInternalFileStatus() {
    return this.fileStatus;
  }

  /**
   * To get the path of the file
   * @return
   */
  @Override
  public edu.iu.dsc.tws.data.fs.Path getPath() {
    return new edu.iu.dsc.tws.data.fs.Path(fileStatus.getPath().toString());
  }

  /**
   * To check whether it is a directory or not.
   * @return
   */
  @SuppressWarnings("deprecation")
  @Override
  public boolean isDir() {
    return fileStatus.isDir();
  }
}
