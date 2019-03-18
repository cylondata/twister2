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
package edu.iu.dsc.tws.comms.shuffle;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public final class MemoryMapUtils {

  public static boolean isOldJDK;
  public static Method clean;

  public static Object theUnsafe;

  static {
    isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
    try {
      if (isOldJDK) {
        clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
        clean.setAccessible(true);
      } else {
        Class unsafeClass;
        try {
          unsafeClass = Class.forName("sun.misc.Unsafe");
        } catch (Exception ex) {
          // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
          // but that method should be added if sun.misc.Unsafe is removed.
          unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
        }
        clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        clean.setAccessible(true);
        Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
        theUnsafeField.setAccessible(true);
        theUnsafe = theUnsafeField.get(null);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private MemoryMapUtils() {
  }

  public static boolean unMapBuffer(MappedByteBuffer buffer)
      throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    if (clean == null) {
      return false;
    }

    if (isOldJDK) {
      Method cleaner = buffer.getClass().getMethod("cleaner");
      cleaner.setAccessible(true);
      clean.invoke(cleaner.invoke(buffer));
      return true;
    } else if (theUnsafe != null) {
      clean.invoke(theUnsafe, buffer);
      return true;
    } else {
      return false;
    }
  }
}
