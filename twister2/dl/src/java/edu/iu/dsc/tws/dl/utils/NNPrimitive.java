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
package edu.iu.dsc.tws.dl.utils;

import java.util.Arrays;

import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;

@SuppressWarnings("NeedBraces")
public final class NNPrimitive {

  private NNPrimitive() {
  }

  /**
   * im2colDouble.
   *
   * @param fInput
   * @param input
   * @param kW
   * @param kH
   * @param dW
   * @param dH
   * @param padLeft
   * @param padTop
   * @param padRight
   * @param padBottom
   * @param outputWidth
   * @param outputHeight
   */
  public static void im2colDouble(
      DenseTensor fInput, DenseTensor input,
      int kW, int kH, int
          dW, int dH, int
          padLeft, int padTop, int padRight, int padBottom, int
          outputWidth, int outputHeight) {

    int nInputPlane = input.size(1);
    int inputHeight = input.size(2);
    int inputWidth = input.size(3);

    double[] inputData = input.storage().toDoubleArray();
    double[] fInputData = fInput.storage().toDoubleArray();

    int k = 0;
    while (k < nInputPlane * kH * kW) {
      int nip = k / (kH * kW);
      int rest = k % (kH * kW);
      int kh = rest / kW;
      int kw = rest % kW;
      int dstOffset = k * outputHeight * outputWidth + fInput.storageOffset() - 1;
      int srcOffset = nip * inputWidth * inputHeight + input.storageOffset() - 1;
      if (padLeft > 0 || padRight > 0 || padTop > 0 || padBottom > 0) {
        int y = 0;
        while (y < outputHeight) {
          int iy = y * dH - padTop + kh;
          if (iy < 0 || iy >= inputHeight) {
            Arrays.fill(fInputData, dstOffset + y * outputWidth,
                dstOffset + (y + 1) * outputWidth, 0);
          } else {
            if (dW == 1) {
              int ix = 0 - padLeft + kw;
              int lpad = Math.max(0, padLeft - kw);
              int rpad = Math.max(0, padRight - (kW - kw - 1));
              if (outputWidth - rpad - lpad <= 0) {
                Arrays.fill(fInputData, dstOffset + y * outputWidth,
                    dstOffset + (y + 1) * outputWidth, 0);
              } else {
                if (lpad > 0) Arrays.fill(fInputData, dstOffset + y * outputWidth,
                    dstOffset + y * outputWidth + lpad, 0);
                System.arraycopy(inputData, srcOffset + iy * inputWidth + ix + lpad, fInputData,
                    dstOffset + y * outputWidth + lpad, outputWidth - rpad - lpad);
                if (rpad > 0) Arrays.fill(fInputData, dstOffset + (y + 1) * outputWidth - rpad,
                    dstOffset + (y + 1) * outputWidth, 0);
              }
            } else {
              int x = 0;
              while (x < outputWidth) {
                int ix = x * dW - padLeft + kw;
                if (ix < 0 || ix >= inputWidth) {
                  fInputData[dstOffset + y * outputWidth + x] = 0;
                } else {
                  fInputData[dstOffset + y * outputWidth + x] =
                      inputData[srcOffset + iy * inputWidth + ix];
                }
                x += 1;
              }
            }
          }
          y += 1;
        }
      } else {
        int y = 0;
        while (y < outputHeight) {
          int iy = y * dH + kh;
          int ix = 0 + kw;
          if (dW == 1) {
            System.arraycopy(inputData, srcOffset + iy * inputWidth + ix,
                fInputData, dstOffset + y * outputWidth, outputWidth);
          } else {
            int x = 0;
            while (x < outputWidth) {
              fInputData[dstOffset + y * outputWidth + x] =
                  inputData[srcOffset + iy * inputWidth + ix + x * dW];
              x += 1;
            }
          }
          y += 1;
        }
      }
      k += 1;
    }
  }

  /**
   * col2imDouble.
   *
   * @param fInput
   * @param input
   * @param kW
   * @param kH
   * @param dW
   * @param dH
   * @param padLeft
   * @param padTop
   * @param padRight
   * @param padBottom
   * @param outputWidth
   * @param outputHeight
   */
  public static void col2imDouble(
      DenseTensor fInput, DenseTensor input,
      int kW, int kH, int
          dW, int dH, int
          padLeft, int padTop, int padRight, int padBottom, int
          outputWidth, int outputHeight) {

    int nInputPlane = input.size(1);
    int inputHeight = input.size(2);
    int inputWidth = input.size(3);

    double[] inputData = input.storage().toDoubleArray();
    double[] fInputData = fInput.storage().toDoubleArray();
    int nPlane = 0;
    while (nPlane < nInputPlane) {
      int kh = 0;
      while (kh < kH) {
        int kw = 0;
        while (kw < kW) {
          int srcOffset = nPlane * (kH * kW * outputHeight * outputWidth)
              +              kh * (kW * outputHeight * outputWidth)
              +              kw * (outputHeight * outputWidth) + fInput.storageOffset() - 1;
          int dstOffset = nPlane * (inputHeight * inputWidth) + input.storageOffset() - 1;
          if (padLeft > 0 || padRight > 0 || padTop > 0 || padBottom > 0) {
            int y = 0;
            while (y < outputHeight) {
              int iy = y * dH - padTop + kh;
              if (iy >= 0 && iy < inputHeight) {
                if (dW == 1) {
                  int ix = 0 - padLeft + kw;
                  int lPad = Math.max(0, padLeft - kw);
                  int rPad = Math.max(0, padRight - (kW - kw - 1));
                  int inputDataOffset = dstOffset + iy * inputWidth + ix + lPad;
                  int fInputDataOffset = srcOffset + y * outputWidth + lPad;
                  int n = outputWidth - lPad - rPad;
                  int i = 0;
                  while (i < n) {
                    inputData[inputDataOffset + i] += fInputData[fInputDataOffset + i];
                    i += 1;
                  }
                } else {
                  int x = 0;
                  while (x < outputWidth) {
                    int ix = x * dW - padLeft + kw;
                    if (ix >= 0 && ix < inputWidth) {
                      inputData[dstOffset + iy * inputWidth + ix] +=
                          fInputData[srcOffset + y * outputWidth + x];
                    }
                    x += 1;
                  }
                }
              }
              y += 1;
            }
          } else {
            int y = 0;
            while (y < outputHeight) {
              int iy = y * dH + kh;
              int ix = 0 + kw;
              if (dW == 1) {
                int i = 0;
                int inputDataOffset = dstOffset + iy * inputWidth + ix;
                int fInputDataOffset = srcOffset + y * outputWidth;
                while (i < outputWidth) {
                  inputData[inputDataOffset + i] += fInputData[fInputDataOffset + i];
                  i += 1;
                }
              } else {
                int x = 0;
                while (x < outputWidth) {
                  inputData[dstOffset + iy * inputWidth + ix + x * dW] +=
                      fInputData[srcOffset + y * outputWidth + x];
                  x += 1;
                }
              }
              y += 1;
            }
          }
          kw += 1;
        }
        kh += 1;
      }
      nPlane += 1;
    }
  }

  /**
   * im2colDoubleNHWC.
   *
   * @param fInput
   * @param input
   * @param kW
   * @param kH
   * @param dW
   * @param dH
   * @param padLeft
   * @param padTop
   * @param padRight
   * @param padBottom
   * @param outputWidth
   * @param outputHeight
   */
  public static void im2colDoubleNHWC(
      DenseTensor fInput, DenseTensor input,
      int kW, int kH, int
          dW, int dH, int
          padLeft, int padTop, int padRight, int padBottom, int
          outputWidth, int outputHeight) {

    // padRight and padBottom are used in the NCHW version but not here,
    // add it to keep api consistent

    int nInputPlane = input.size(3);
    int inputHeight = input.size(1);
    int inputWidth = input.size(2);

    double[] inputData = input.storage().toDoubleArray();
    double[] fInputData = fInput.storage().toDoubleArray();

    int srcOffset = input.storageOffset() - 1;
    int destOffset = fInput.storageOffset() - 1;

    int hPad = -padTop;
    int fInputCount = 0;
    int h = 0;
    while (h < outputHeight) {
      int wPad = -padLeft;
      int w = 0;
      while (w < outputWidth) {
        int ih = hPad;
        while (ih < hPad + kH) {
          int iw = wPad;
          while (iw < wPad + kW) {
            if (ih >= 0 && ih < inputHeight && iw >= 0 && iw < inputWidth) {
              int src = srcOffset + (ih * inputWidth + iw) * nInputPlane;
              int dest = destOffset + fInputCount;
              int n = Math.min(inputWidth, wPad + kW) - iw;
              System.arraycopy(inputData, src,
                  fInputData, dest, nInputPlane * n);
              fInputCount = fInputCount + nInputPlane * n;
              iw = iw + n;
            } else {
              int n;
              if (ih < 0 || ih >= inputHeight || iw >= inputWidth) {
                n = wPad + kW - iw;
              } else {
                n = 0 - iw;
              }
              int fromIndex = destOffset + fInputCount;
              int toIndex = fromIndex + nInputPlane * n;
              Arrays.fill(fInputData, fromIndex, toIndex, 0.0);
              fInputCount = fInputCount + nInputPlane * n;
              iw = iw + n;
            }
          }
          ih = ih + 1;
        }
        w = w + 1;
        wPad = wPad + dW;
      }
      h = h + 1;
      hPad = hPad + dH;
    }
  }

  /**
   * col2imDoubleNHWC.
   *
   * @param fInput
   * @param input
   * @param kW
   * @param kH
   * @param dW
   * @param dH
   * @param padLeft
   * @param padTop
   * @param padRight
   * @param padBottom
   * @param outputWidth
   * @param outputHeight
   */
  public static void col2imDoubleNHWC(
      DenseTensor fInput, DenseTensor input,
      int kW, int kH, int
          dW, int dH, int
          padLeft, int padTop, int padRight, int padBottom, int
          outputWidth, int outputHeight) {

    // padRight and padBottom are used in the NCHW version but not here,
    // add it to keep api consistent

    int nInputPlane = input.size(3);
    int inputHeight = input.size(1);
    int inputWidth = input.size(2);

    double[] inputData = input.storage().toDoubleArray();
    int inputOffset = input.storageOffset() - 1;
    double[] fInputData = fInput.storage().toDoubleArray();
    int fInputOffset = fInput.storageOffset() - 1;
    int hPad = -padTop;
    int h = 0;
    int fInputCount = 0;
    while (h < outputHeight) {
      int wPad = -padLeft;
      int w = 0;
      while (w < outputWidth) {
        int ih = hPad;
        while (ih < hPad + kH) {
          int iw = wPad;
          while (iw < wPad + kW) {
            if (ih >= 0 && ih < inputHeight && iw >= 0 && iw < inputWidth) {
              int dataImPatch = inputOffset + (ih * inputWidth + iw) * nInputPlane;
              int i = 0;
              while (i < nInputPlane) {
                inputData[dataImPatch + i] += fInputData[fInputOffset + fInputCount];
                fInputCount = fInputCount + 1;
                i = i + 1;
              }
            } else {
              fInputCount = fInputCount + nInputPlane;
            }
            iw = iw + 1;
          }
          ih = ih + 1;
        }
        w = w + 1;
        wPad = wPad + dW;
      }
      h = h + 1;
      hPad = hPad + dH;
    }
  }
}
