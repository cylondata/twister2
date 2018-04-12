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
package edu.iu.dsc.tws.examples.utils;

import java.security.SecureRandom;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;

public class RandomString {
  private String words = "The original and reference implementation Java compilers "
      + "virtual machines and class libraries were originally released by Sun under proprietary"
      + "licenses As of May 2007 in compliance with the specifications of the Java Community"
      + "Process Sun relicensed most of its Java technologies under the GNU General Public License"
      + "Others have also developed alternative implementations of these Sun technologies such "
      + "as the GNU Compiler for Java";
  /**
   * Generate a random string.
   */
  public String nextString() {
    for (int idx = 0; idx < buf.length; ++idx) {
      buf[idx] = symbols[random.nextInt(symbols.length)];
    }
    return new String(buf);
  }

  public String nextRandomSizeString() {
    int next = (int) (random.nextDouble() * maxLength);
    char[] chars = new char[next];
    for (int idx = 0; idx < chars.length; ++idx) {
      chars[idx] = symbols[random.nextInt(symbols.length)];
    }
    return new String(chars);
  }

  public String nextFixedRandomString() {
    int next = random.nextInt(wordsArrays.length);
    return wordsArrays[next];
  }

  public static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public static final String LOWER = UPPER.toLowerCase(Locale.ROOT);

  public static final String DIGITS = "0123456789";

  public static final String ALPHANUM = UPPER + LOWER + DIGITS;

  private Random random;

  private char[] symbols;

  private char[] buf;

  private int maxLength;

  private String[] wordsArrays;

  public RandomString() {
    wordsArrays = words.split(" ");
    this.random = new Random();
  }

  public RandomString(int length, Random random, String symbols) {
    if (length < 1) {
      throw new IllegalArgumentException();
    }
    if (symbols.length() < 2) {
      throw new IllegalArgumentException();
    }
    this.random = Objects.requireNonNull(random);
    this.symbols = symbols.toCharArray();
    this.buf = new char[length];
    this.maxLength = length;
  }

  /**
   * Create an alphanumeric string generator.
   */
  public RandomString(int length, Random random) {
    this(length, random, ALPHANUM);
  }

  /**
   * Create an alphanumeric strings from a secure generator.
   */
  public RandomString(int length) {
    this(length, new SecureRandom());
  }

  public static void main(String[] args) {
    RandomString r = new RandomString(100, new Random(System.nanoTime()), ALPHANUM);
    for (int i = 0; i < 1000; i++) {
      System.out.println(r.nextRandomSizeString());
    }
  }

  public static String getUpper() {
    return UPPER;
  }

  public static String getLower() {
    return LOWER;
  }

  public static String getDigits() {
    return DIGITS;
  }

  public static String getAlphanum() {
    return ALPHANUM;
  }

  public Random getRandom() {
    return random;
  }

  public char[] getSymbols() {
    return symbols;
  }

  public char[] getBuf() {
    return buf;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public void setMaxLength(int maxLength) {
    this.maxLength = maxLength;
  }

  public void setRandom(Random random) {
    this.random = random;
  }

  public void setSymbols(char[] symbols) {
    this.symbols = symbols;
  }

  public void setBuf(char[] buf) {
    this.buf = buf;
  }
}
