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
package edu.iu.dsc.tws.rsched.uploaders.s3;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;

public final class S3Context {

  public static final String S3_BUCKET_NAME = "twister2.s3.bucket.name";

  // job package link will be available this much time
  // by default, it is 2 hours
  public static final long S3_LINK_EXPIRATION_DURATION_DEFAULT = 3600 * 2;
  public static final String S3_LINK_EXPIRATION_DURATION
      = "twister2.s3.link.expiration.duration.sec";

  private S3Context() { }

  public static String uploaderScript(Config config) {
    return Context.twister2Home(config) + "/conf/scripts/s3Uploader.sh";
  }

  public static String urlGenScript(Config config) {
    return Context.twister2Home(config) + "/conf/scripts/s3UrlGen.sh";
  }

  public static String s3BucketName(Config config) {
    return config.getStringValue(S3_BUCKET_NAME);
  }

  public static long linkExpirationDuration(Config config) {
    return config.getLongValue(S3_LINK_EXPIRATION_DURATION, S3_LINK_EXPIRATION_DURATION_DEFAULT);
  }


}
