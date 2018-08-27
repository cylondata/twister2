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
package edu.iu.dsc.tws.rsched.uploaders;

import java.net.URI;
import java.net.URISyntaxException;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.exceptions.UploaderException;
import edu.iu.dsc.tws.rsched.interfaces.IUploader;

/**
 * a class to use when no uploading is necessary
 */
public class NullUploader implements IUploader {

  /**
   * Initialize the uploader
   */
  public void initialize(Config config) {
    // nothing to do
  }

  /**
   * return some useless but valid URI
   */
  public URI uploadPackage(String sourceLocation) throws UploaderException {
    try {
      return new URI("/root/.twister2/repository");
    } catch (URISyntaxException e) {
      throw new UploaderException("Don't know how to convert to URI");
    }
  }

  /**
   * If subsequent stages fail, undo will be called to free resources used by
   * uploading package. Ideally, this should try to remove the uploaded package.
   */
  public boolean undo() {
    return true;
  }

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the uploader
   * <p>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  public void close() {
    // nothing to do
  }
}
