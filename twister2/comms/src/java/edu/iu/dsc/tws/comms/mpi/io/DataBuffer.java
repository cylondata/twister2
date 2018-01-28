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
package edu.iu.dsc.tws.comms.mpi.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public class DataBuffer {
  public ByteBuffer slice() {
    return null;
  }

  public ByteBuffer duplicate() {
    return null;
  }

  public ByteBuffer asReadOnlyBuffer() {
    return null;
  }

  public byte get() {
    return 0;
  }

  public ByteBuffer put(byte b) {
    return null;
  }

  public byte get(int index) {
    return 0;
  }

  public ByteBuffer put(int index, byte b) {
    return null;
  }

  public ByteBuffer compact() {
    return null;
  }

  public boolean isReadOnly() {
    return false;
  }

  public char getChar() {
    return 0;
  }

  public ByteBuffer putChar(char value) {
    return null;
  }

  public char getChar(int index) {
    return 0;
  }

  public ByteBuffer putChar(int index, char value) {
    return null;
  }

  public CharBuffer asCharBuffer() {
    return null;
  }

  public short getShort() {
    return 0;
  }

  public ByteBuffer putShort(short value) {
    return null;
  }

  public short getShort(int index) {
    return 0;
  }

  public ByteBuffer putShort(int index, short value) {
    return null;
  }

  public ShortBuffer asShortBuffer() {
    return null;
  }

  public int getInt() {
    return 0;
  }

  public ByteBuffer putInt(int value) {
    return null;
  }

  public int getInt(int index) {
    return 0;
  }

  public ByteBuffer putInt(int index, int value) {
    return null;
  }

  public IntBuffer asIntBuffer() {
    return null;
  }

  public long getLong() {
    return 0;
  }

  public ByteBuffer putLong(long value) {
    return null;
  }

  public long getLong(int index) {
    return 0;
  }

  public ByteBuffer putLong(int index, long value) {
    return null;
  }

  public LongBuffer asLongBuffer() {
    return null;
  }

  public float getFloat() {
    return 0;
  }

  public ByteBuffer putFloat(float value) {
    return null;
  }

  public float getFloat(int index) {
    return 0;
  }

  public ByteBuffer putFloat(int index, float value) {
    return null;
  }

  public FloatBuffer asFloatBuffer() {
    return null;
  }

  public double getDouble() {
    return 0;
  }

  public ByteBuffer putDouble(double value) {
    return null;
  }

  public double getDouble(int index) {
    return 0;
  }

  public ByteBuffer putDouble(int index, double value) {
    return null;
  }

  public DoubleBuffer asDoubleBuffer() {
    return null;
  }
}
