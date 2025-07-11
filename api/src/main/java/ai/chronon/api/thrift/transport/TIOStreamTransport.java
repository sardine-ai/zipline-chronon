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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import ai.chronon.api.thrift.TConfiguration;
import ai.chronon.api.thrift.transport.TEndpointTransport;
import ai.chronon.api.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the most commonly used base transport. It takes an InputStream or an OutputStream or both
 * and uses it/them to perform transport operations. This allows for compatibility with all the nice
 * constructs Java already has to provide a variety of types of streams.
 */
public class TIOStreamTransport extends TEndpointTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TIOStreamTransport.class.getName());

  /** Underlying inputStream */
  protected InputStream inputStream_ = null;

  /** Underlying outputStream */
  protected OutputStream outputStream_ = null;

  /**
   * Subclasses can invoke the default constructor and then assign the input streams in the open
   * method.
   */
  protected TIOStreamTransport(TConfiguration config) throws ai.chronon.api.thrift.transport.TTransportException {
    super(config);
  }

  /**
   * Subclasses can invoke the default constructor and then assign the input streams in the open
   * method.
   */
  protected TIOStreamTransport() throws ai.chronon.api.thrift.transport.TTransportException {
    super(new TConfiguration());
  }

  /**
   * Input stream constructor, constructs an input only transport.
   *
   * @param config
   * @param is Input stream to read from
   */
  public TIOStreamTransport(TConfiguration config, InputStream is) throws ai.chronon.api.thrift.transport.TTransportException {
    super(config);
    inputStream_ = is;
  }

  /**
   * Input stream constructor, constructs an input only transport.
   *
   * @param is Input stream to read from
   */
  public TIOStreamTransport(InputStream is) throws ai.chronon.api.thrift.transport.TTransportException {
    super(new TConfiguration());
    inputStream_ = is;
  }

  /**
   * Output stream constructor, constructs an output only transport.
   *
   * @param config
   * @param os Output stream to write to
   */
  public TIOStreamTransport(TConfiguration config, OutputStream os) throws ai.chronon.api.thrift.transport.TTransportException {
    super(config);
    outputStream_ = os;
  }

  /**
   * Output stream constructor, constructs an output only transport.
   *
   * @param os Output stream to write to
   */
  public TIOStreamTransport(OutputStream os) throws ai.chronon.api.thrift.transport.TTransportException {
    super(new TConfiguration());
    outputStream_ = os;
  }

  /**
   * Two-way stream constructor.
   *
   * @param config
   * @param is Input stream to read from
   * @param os Output stream to read from
   */
  public TIOStreamTransport(TConfiguration config, InputStream is, OutputStream os)
      throws ai.chronon.api.thrift.transport.TTransportException {
    super(config);
    inputStream_ = is;
    outputStream_ = os;
  }

  /**
   * Two-way stream constructor.
   *
   * @param is Input stream to read from
   * @param os Output stream to read from
   */
  public TIOStreamTransport(InputStream is, OutputStream os) throws ai.chronon.api.thrift.transport.TTransportException {
    super(new TConfiguration());
    inputStream_ = is;
    outputStream_ = os;
  }

  /**
   * @return false after close is called.
   */
  public boolean isOpen() {
    return inputStream_ != null || outputStream_ != null;
  }

  /** The streams must already be open. This method does nothing. */
  public void open() throws ai.chronon.api.thrift.transport.TTransportException {}

  /** Closes both the input and output streams. */
  public void close() {
    try {
      if (inputStream_ != null) {
        try {
          inputStream_.close();
        } catch (IOException iox) {
          LOGGER.warn("Error closing input stream.", iox);
        }
      }
      if (outputStream_ != null) {
        try {
          outputStream_.close();
        } catch (IOException iox) {
          LOGGER.warn("Error closing output stream.", iox);
        }
      }
    } finally {
      inputStream_ = null;
      outputStream_ = null;
    }
  }

  /** Reads from the underlying input stream if not null. */
  public int read(byte[] buf, int off, int len) throws ai.chronon.api.thrift.transport.TTransportException {
    if (inputStream_ == null) {
      throw new ai.chronon.api.thrift.transport.TTransportException(
          ai.chronon.api.thrift.transport.TTransportException.NOT_OPEN, "Cannot read from null inputStream");
    }
    int bytesRead;
    try {
      bytesRead = inputStream_.read(buf, off, len);
    } catch (SocketTimeoutException ste) {
      throw new ai.chronon.api.thrift.transport.TTransportException(ai.chronon.api.thrift.transport.TTransportException.TIMED_OUT, ste);
    } catch (IOException iox) {
      throw new ai.chronon.api.thrift.transport.TTransportException(ai.chronon.api.thrift.transport.TTransportException.UNKNOWN, iox);
    }
    if (bytesRead < 0) {
      throw new ai.chronon.api.thrift.transport.TTransportException(ai.chronon.api.thrift.transport.TTransportException.END_OF_FILE, "Socket is closed by peer.");
    }
    return bytesRead;
  }

  /** Writes to the underlying output stream if not null. */
  public void write(byte[] buf, int off, int len) throws ai.chronon.api.thrift.transport.TTransportException {
    if (outputStream_ == null) {
      throw new ai.chronon.api.thrift.transport.TTransportException(
          ai.chronon.api.thrift.transport.TTransportException.NOT_OPEN, "Cannot write to null outputStream");
    }
    try {
      outputStream_.write(buf, off, len);
    } catch (IOException iox) {
      throw new ai.chronon.api.thrift.transport.TTransportException(ai.chronon.api.thrift.transport.TTransportException.UNKNOWN, iox);
    }
  }

  /** Flushes the underlying output stream if not null. */
  public void flush() throws ai.chronon.api.thrift.transport.TTransportException {
    if (outputStream_ == null) {
      throw new ai.chronon.api.thrift.transport.TTransportException(ai.chronon.api.thrift.transport.TTransportException.NOT_OPEN, "Cannot flush null outputStream");
    }
    try {
      outputStream_.flush();

      resetConsumedMessageSize(-1);

    } catch (IOException iox) {
      throw new ai.chronon.api.thrift.transport.TTransportException(TTransportException.UNKNOWN, iox);
    }
  }
}
