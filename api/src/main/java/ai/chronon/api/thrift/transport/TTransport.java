/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ai.chronon.api.thrift.transport;

import java.io.Closeable;
import java.nio.ByteBuffer;
import ai.chronon.api.thrift.TConfiguration;
import ai.chronon.api.thrift.transport.TTransportException;

/**
 * Generic class that encapsulates the I/O layer. This is basically a thin wrapper around the
 * combined functionality of Java input/output streams.
 */
public abstract class TTransport implements Closeable {

  /**
   * Queries whether the transport is open.
   *
   * @return True if the transport is open.
   */
  public abstract boolean isOpen();

  /**
   * Is there more data to be read?
   *
   * @return True if the remote side is still alive and feeding us
   */
  public boolean peek() {
    return isOpen();
  }

  /**
   * Opens the transport for reading/writing.
   *
   * @throws TTransportException if the transport could not be opened
   */
  public abstract void open() throws TTransportException;

  /** Closes the transport. */
  public abstract void close();

  /**
   * Reads a sequence of bytes from this channel into the given buffer. An attempt is made to read
   * up to the number of bytes remaining in the buffer, that is, dst.remaining(), at the moment this
   * method is invoked. Upon return the buffer's position will move forward the number of bytes
   * read; its limit will not have changed. Subclasses are encouraged to provide a more efficient
   * implementation of this method.
   *
   * @param dst The buffer into which bytes are to be transferred
   * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
   * @throws TTransportException if there was an error reading data
   */
  public int read(ByteBuffer dst) throws TTransportException {
    byte[] arr = new byte[dst.remaining()];
    int n = read(arr, 0, arr.length);
    dst.put(arr, 0, n);
    return n;
  }

  /**
   * Reads up to len bytes into buffer buf, starting at offset off.
   *
   * @param buf Array to read into
   * @param off Index to start reading at
   * @param len Maximum number of bytes to read
   * @return The number of bytes actually read
   * @throws TTransportException if there was an error reading data
   */
  public abstract int read(byte[] buf, int off, int len) throws TTransportException;

  /**
   * Guarantees that all of len bytes are actually read off the transport.
   *
   * @param buf Array to read into
   * @param off Index to start reading at
   * @param len Maximum number of bytes to read
   * @return The number of bytes actually read, which must be equal to len
   * @throws TTransportException if there was an error reading data
   */
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    int got = 0;
    int ret = 0;
    while (got < len) {
      ret = read(buf, off + got, len - got);
      if (ret <= 0) {
        throw new TTransportException(
            "Cannot read. Remote side has closed. Tried to read "
                + len
                + " bytes, but only got "
                + got
                + " bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)");
      }
      got += ret;
    }
    return got;
  }

  /**
   * Writes the buffer to the output
   *
   * @param buf The output data buffer
   * @throws TTransportException if an error occurs writing data
   */
  public void write(byte[] buf) throws TTransportException {
    write(buf, 0, buf.length);
  }

  /**
   * Writes up to len bytes from the buffer.
   *
   * @param buf The output data buffer
   * @param off The offset to start writing from
   * @param len The number of bytes to write
   * @throws TTransportException if there was an error writing data
   */
  public abstract void write(byte[] buf, int off, int len) throws TTransportException;

  /**
   * Writes a sequence of bytes to the buffer. An attempt is made to write all remaining bytes in
   * the buffer, that is, src.remaining(), at the moment this method is invoked. Upon return the
   * buffer's position will updated; its limit will not have changed. Subclasses are encouraged to
   * provide a more efficient implementation of this method.
   *
   * @param src The buffer from which bytes are to be retrieved
   * @return The number of bytes written, possibly zero
   * @throws TTransportException if there was an error writing data
   */
  public int write(ByteBuffer src) throws TTransportException {
    byte[] arr = new byte[src.remaining()];
    src.get(arr);
    write(arr, 0, arr.length);
    return arr.length;
  }

  /**
   * Flush any pending data out of a transport buffer.
   *
   * @throws TTransportException if there was an error writing out data.
   */
  public void flush() throws TTransportException {}

  /**
   * Access the protocol's underlying buffer directly. If this is not a buffered transport, return
   * null.
   *
   * @return protocol's Underlying buffer
   */
  public byte[] getBuffer() {
    return null;
  }

  /**
   * Return the index within the underlying buffer that specifies the next spot that should be read
   * from.
   *
   * @return index within the underlying buffer that specifies the next spot that should be read
   *     from
   */
  public int getBufferPosition() {
    return 0;
  }

  /**
   * Get the number of bytes remaining in the underlying buffer. Returns -1 if this is a
   * non-buffered transport.
   *
   * @return the number of bytes remaining in the underlying buffer. <br>
   *     Returns -1 if this is a non-buffered transport.
   */
  public int getBytesRemainingInBuffer() {
    return -1;
  }

  /**
   * Consume len bytes from the underlying buffer.
   *
   * @param len the number of bytes to consume from the underlying buffer.
   */
  public void consumeBuffer(int len) {}

  public abstract TConfiguration getConfiguration();

  public abstract void updateKnownMessageSize(long size) throws TTransportException;

  public abstract void checkReadBytesAvailable(long numBytes) throws TTransportException;
}
