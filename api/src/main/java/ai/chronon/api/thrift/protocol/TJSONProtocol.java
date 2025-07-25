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

package ai.chronon.api.thrift.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Stack;
import java.util.UUID;
import ai.chronon.api.thrift.TByteArrayOutputStream;
import ai.chronon.api.thrift.TException;
import ai.chronon.api.thrift.protocol.TBase64Utils;
import ai.chronon.api.thrift.protocol.TField;
import ai.chronon.api.thrift.protocol.TList;
import ai.chronon.api.thrift.protocol.TMap;
import ai.chronon.api.thrift.protocol.TMessage;
import ai.chronon.api.thrift.protocol.TProtocol;
import ai.chronon.api.thrift.protocol.TProtocolException;
import ai.chronon.api.thrift.protocol.TProtocolFactory;
import ai.chronon.api.thrift.protocol.TSet;
import ai.chronon.api.thrift.protocol.TStruct;
import ai.chronon.api.thrift.protocol.TType;
import ai.chronon.api.thrift.transport.TTransport;
import ai.chronon.api.thrift.transport.TTransportException;

/**
 * JSON protocol implementation for thrift.
 *
 * <p>This is a full-featured protocol supporting write and read.
 *
 * <p>Please see the C++ class header for a detailed description of the protocol's wire format.
 */
public class TJSONProtocol extends ai.chronon.api.thrift.protocol.TProtocol {

  /** Factory for JSON protocol objects */
  public static class Factory implements TProtocolFactory {
    protected boolean fieldNamesAsString_ = false;

    public Factory() {}

    public Factory(boolean fieldNamesAsString) {
      fieldNamesAsString_ = fieldNamesAsString;
    }

    public TProtocol getProtocol(TTransport trans) {
      return new TJSONProtocol(trans, fieldNamesAsString_);
    }
  }

  private static final byte[] COMMA = new byte[] {','};
  private static final byte[] COLON = new byte[] {':'};
  private static final byte[] LBRACE = new byte[] {'{'};
  private static final byte[] RBRACE = new byte[] {'}'};
  private static final byte[] LBRACKET = new byte[] {'['};
  private static final byte[] RBRACKET = new byte[] {']'};
  private static final byte[] QUOTE = new byte[] {'"'};
  private static final byte[] BACKSLASH = new byte[] {'\\'};

  private static final byte[] ESCSEQ = new byte[] {'\\', 'u', '0', '0'};

  private static final long VERSION = 1;

  private static final byte[] JSON_CHAR_TABLE = {
    /*  0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F */
    0, 0, 0, 0, 0, 0, 0, 0, 'b', 't', 'n', 0, 'f', 'r', 0, 0, // 0
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 1
    1, 1, '"', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 2
  };

  private static final String ESCAPE_CHARS = "\"\\/bfnrt";

  private static final byte[] ESCAPE_CHAR_VALS = {
    '"', '\\', '/', '\b', '\f', '\n', '\r', '\t',
  };

  private static final int DEF_STRING_SIZE = 16;

  private static final byte[] NAME_BOOL = new byte[] {'t', 'f'};
  private static final byte[] NAME_BYTE = new byte[] {'i', '8'};
  private static final byte[] NAME_I16 = new byte[] {'i', '1', '6'};
  private static final byte[] NAME_I32 = new byte[] {'i', '3', '2'};
  private static final byte[] NAME_I64 = new byte[] {'i', '6', '4'};
  private static final byte[] NAME_UUID = new byte[] {'u', 'i', 'd'};
  private static final byte[] NAME_DOUBLE = new byte[] {'d', 'b', 'l'};
  private static final byte[] NAME_STRUCT = new byte[] {'r', 'e', 'c'};
  private static final byte[] NAME_STRING = new byte[] {'s', 't', 'r'};
  private static final byte[] NAME_MAP = new byte[] {'m', 'a', 'p'};
  private static final byte[] NAME_LIST = new byte[] {'l', 's', 't'};
  private static final byte[] NAME_SET = new byte[] {'s', 'e', 't'};

  private static final ai.chronon.api.thrift.protocol.TStruct ANONYMOUS_STRUCT = new ai.chronon.api.thrift.protocol.TStruct();

  private static byte[] getTypeNameForTypeID(byte typeID) throws TException {
    switch (typeID) {
      case ai.chronon.api.thrift.protocol.TType.BOOL:
        return NAME_BOOL;
      case ai.chronon.api.thrift.protocol.TType.BYTE:
        return NAME_BYTE;
      case ai.chronon.api.thrift.protocol.TType.I16:
        return NAME_I16;
      case ai.chronon.api.thrift.protocol.TType.I32:
        return NAME_I32;
      case ai.chronon.api.thrift.protocol.TType.I64:
        return NAME_I64;
      case ai.chronon.api.thrift.protocol.TType.UUID:
        return NAME_UUID;
      case ai.chronon.api.thrift.protocol.TType.DOUBLE:
        return NAME_DOUBLE;
      case ai.chronon.api.thrift.protocol.TType.STRING:
        return NAME_STRING;
      case ai.chronon.api.thrift.protocol.TType.STRUCT:
        return NAME_STRUCT;
      case ai.chronon.api.thrift.protocol.TType.MAP:
        return NAME_MAP;
      case ai.chronon.api.thrift.protocol.TType.SET:
        return NAME_SET;
      case ai.chronon.api.thrift.protocol.TType.LIST:
        return NAME_LIST;
      default:
        throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED, "Unrecognized type");
    }
  }

  private static byte getTypeIDForTypeName(byte[] name) throws TException {
    byte result = ai.chronon.api.thrift.protocol.TType.STOP;
    if (name.length > 1) {
      switch (name[0]) {
        case 'd':
          result = ai.chronon.api.thrift.protocol.TType.DOUBLE;
          break;
        case 'i':
          switch (name[1]) {
            case '8':
              result = ai.chronon.api.thrift.protocol.TType.BYTE;
              break;
            case '1':
              result = ai.chronon.api.thrift.protocol.TType.I16;
              break;
            case '3':
              result = ai.chronon.api.thrift.protocol.TType.I32;
              break;
            case '6':
              result = ai.chronon.api.thrift.protocol.TType.I64;
              break;
          }
          break;
        case 'l':
          result = ai.chronon.api.thrift.protocol.TType.LIST;
          break;
        case 'm':
          result = ai.chronon.api.thrift.protocol.TType.MAP;
          break;
        case 'r':
          result = ai.chronon.api.thrift.protocol.TType.STRUCT;
          break;
        case 's':
          if (name[1] == 't') {
            result = ai.chronon.api.thrift.protocol.TType.STRING;
          } else if (name[1] == 'e') {
            result = ai.chronon.api.thrift.protocol.TType.SET;
          }
          break;
        case 'u':
          result = ai.chronon.api.thrift.protocol.TType.UUID;
          break;
        case 't':
          result = ai.chronon.api.thrift.protocol.TType.BOOL;
          break;
      }
    }
    if (result == ai.chronon.api.thrift.protocol.TType.STOP) {
      throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED, "Unrecognized type");
    }
    return result;
  }

  // Base class for tracking JSON contexts that may require inserting/reading
  // additional JSON syntax characters
  // This base context does nothing.
  protected static class JSONBaseContext {
    protected void write() throws TException {}

    protected void read() throws TException {}

    protected boolean escapeNum() {
      return false;
    }
  }

  // Context for JSON lists. Will insert/read commas before each item except
  // for the first one
  protected class JSONListContext extends JSONBaseContext {
    private boolean first_ = true;

    @Override
    protected void write() throws TException {
      if (first_) {
        first_ = false;
      } else {
        trans_.write(COMMA);
      }
    }

    @Override
    protected void read() throws TException {
      if (first_) {
        first_ = false;
      } else {
        readJSONSyntaxChar(COMMA);
      }
    }
  }

  // Context for JSON records. Will insert/read colons before the value portion
  // of each record pair, and commas before each key except the first. In
  // addition, will indicate that numbers in the key position need to be
  // escaped in quotes (since JSON keys must be strings).
  protected class JSONPairContext extends JSONBaseContext {
    private boolean first_ = true;
    private boolean colon_ = true;

    @Override
    protected void write() throws TException {
      if (first_) {
        first_ = false;
        colon_ = true;
      } else {
        trans_.write(colon_ ? COLON : COMMA);
        colon_ = !colon_;
      }
    }

    @Override
    protected void read() throws TException {
      if (first_) {
        first_ = false;
        colon_ = true;
      } else {
        readJSONSyntaxChar(colon_ ? COLON : COMMA);
        colon_ = !colon_;
      }
    }

    @Override
    protected boolean escapeNum() {
      return colon_;
    }
  }

  // Holds up to one byte from the transport
  protected class LookaheadReader {

    private boolean hasData_;
    private final byte[] data_ = new byte[1];

    // Return and consume the next byte to be read, either taking it from the
    // data buffer if present or getting it from the transport otherwise.
    protected byte read() throws TException {
      if (hasData_) {
        hasData_ = false;
      } else {
        trans_.readAll(data_, 0, 1);
      }
      return data_[0];
    }

    // Return the next byte to be read without consuming, filling the data
    // buffer if it has not been filled already.
    protected byte peek() throws TException {
      if (!hasData_) {
        trans_.readAll(data_, 0, 1);
      }
      hasData_ = true;
      return data_[0];
    }
  }

  // Stack of nested contexts that we may be in
  private final Stack<JSONBaseContext> contextStack_ = new Stack<>();

  // Current context that we are in
  private JSONBaseContext context_ = new JSONBaseContext();

  // Reader that manages a 1-byte buffer
  private LookaheadReader reader_ = new LookaheadReader();

  // Write out the TField names as a string instead of the default integer value
  private boolean fieldNamesAsString_ = false;

  // Push a new JSON context onto the stack.
  private void pushContext(JSONBaseContext c) {
    contextStack_.push(context_);
    context_ = c;
  }

  // Pop the last JSON context off the stack
  private void popContext() {
    context_ = contextStack_.pop();
  }

  // Reset the context stack to its initial state
  private void resetContext() {
    while (!contextStack_.isEmpty()) {
      popContext();
    }
  }

  /** Constructor */
  public TJSONProtocol(TTransport trans) {
    super(trans);
  }

  public TJSONProtocol(TTransport trans, boolean fieldNamesAsString) {
    super(trans);
    fieldNamesAsString_ = fieldNamesAsString;
  }

  @Override
  public void reset() {
    contextStack_.clear();
    context_ = new JSONBaseContext();
    reader_ = new LookaheadReader();
  }

  // Temporary buffer used by several methods
  private final byte[] tmpbuf_ = new byte[4];

  // Read a byte that must match b[0]; otherwise an exception is thrown.
  // Marked protected to avoid synthetic accessor in JSONListContext.read
  // and JSONPairContext.read
  protected void readJSONSyntaxChar(byte[] b) throws TException {
    byte ch = reader_.read();
    if (ch != b[0]) {
      throw new TProtocolException(
          TProtocolException.INVALID_DATA, "Unexpected character:" + (char) ch);
    }
  }

  // Convert a byte containing a hex char ('0'-'9' or 'a'-'f') into its
  // corresponding hex value
  private static byte hexVal(byte ch) throws TException {
    if ((ch >= '0') && (ch <= '9')) {
      return (byte) ((char) ch - '0');
    } else if ((ch >= 'a') && (ch <= 'f')) {
      return (byte) ((char) ch - 'a' + 10);
    } else {
      throw new TProtocolException(TProtocolException.INVALID_DATA, "Expected hex character");
    }
  }

  // Convert a byte containing a hex value to its corresponding hex character
  private static byte hexChar(byte val) {
    val &= 0x0F;
    if (val < 10) {
      return (byte) ((char) val + '0');
    } else {
      return (byte) ((char) (val - 10) + 'a');
    }
  }

  // Write the bytes in array buf as a JSON characters, escaping as needed
  private void writeJSONString(byte[] b) throws TException {
    context_.write();
    trans_.write(QUOTE);
    int len = b.length;
    for (int i = 0; i < len; i++) {
      if ((b[i] & 0x00FF) >= 0x30) {
        if (b[i] == BACKSLASH[0]) {
          trans_.write(BACKSLASH);
          trans_.write(BACKSLASH);
        } else {
          trans_.write(b, i, 1);
        }
      } else {
        tmpbuf_[0] = JSON_CHAR_TABLE[b[i]];
        if (tmpbuf_[0] == 1) {
          trans_.write(b, i, 1);
        } else if (tmpbuf_[0] > 1) {
          trans_.write(BACKSLASH);
          trans_.write(tmpbuf_, 0, 1);
        } else {
          trans_.write(ESCSEQ);
          tmpbuf_[0] = hexChar((byte) (b[i] >> 4));
          tmpbuf_[1] = hexChar(b[i]);
          trans_.write(tmpbuf_, 0, 2);
        }
      }
    }
    trans_.write(QUOTE);
  }

  // Write out number as a JSON value. If the context dictates so, it will be
  // wrapped in quotes to output as a JSON string.
  private void writeJSONInteger(long num) throws TException {
    context_.write();
    String str = Long.toString(num);
    boolean escapeNum = context_.escapeNum();
    if (escapeNum) {
      trans_.write(QUOTE);
    }
    byte[] buf = str.getBytes(StandardCharsets.UTF_8);
    trans_.write(buf);
    if (escapeNum) {
      trans_.write(QUOTE);
    }
  }

  // Write out a double as a JSON value. If it is NaN or infinity or if the
  // context dictates escaping, write out as JSON string.
  private void writeJSONDouble(double num) throws TException {
    context_.write();
    String str = Double.toString(num);
    boolean special = false;
    switch (str.charAt(0)) {
      case 'N': // NaN
      case 'I': // Infinity
        special = true;
        break;
      case '-':
        if (str.charAt(1) == 'I') { // -Infinity
          special = true;
        }
        break;
      default:
        break;
    }

    boolean escapeNum = special || context_.escapeNum();
    if (escapeNum) {
      trans_.write(QUOTE);
    }
    byte[] b = str.getBytes(StandardCharsets.UTF_8);
    trans_.write(b, 0, b.length);
    if (escapeNum) {
      trans_.write(QUOTE);
    }
  }

  // Write out contents of byte array b as a JSON string with base-64 encoded
  // data
  private void writeJSONBase64(byte[] b, int offset, int length) throws TException {
    context_.write();
    trans_.write(QUOTE);
    int len = length;
    int off = offset;
    while (len >= 3) {
      // Encode 3 bytes at a time
      ai.chronon.api.thrift.protocol.TBase64Utils.encode(b, off, 3, tmpbuf_, 0);
      trans_.write(tmpbuf_, 0, 4);
      off += 3;
      len -= 3;
    }
    if (len > 0) {
      // Encode remainder
      ai.chronon.api.thrift.protocol.TBase64Utils.encode(b, off, len, tmpbuf_, 0);
      trans_.write(tmpbuf_, 0, len + 1);
    }
    trans_.write(QUOTE);
  }

  private void writeJSONObjectStart() throws TException {
    context_.write();
    trans_.write(LBRACE);
    pushContext(new JSONPairContext());
  }

  private void writeJSONObjectEnd() throws TException {
    popContext();
    trans_.write(RBRACE);
  }

  private void writeJSONArrayStart() throws TException {
    context_.write();
    trans_.write(LBRACKET);
    pushContext(new JSONListContext());
  }

  private void writeJSONArrayEnd() throws TException {
    popContext();
    trans_.write(RBRACKET);
  }

  @Override
  public void writeMessageBegin(ai.chronon.api.thrift.protocol.TMessage message) throws TException {
    resetContext(); // THRIFT-3743
    writeJSONArrayStart();
    writeJSONInteger(VERSION);
    byte[] b = message.name.getBytes(StandardCharsets.UTF_8);
    writeJSONString(b);
    writeJSONInteger(message.type);
    writeJSONInteger(message.seqid);
  }

  @Override
  public void writeMessageEnd() throws TException {
    writeJSONArrayEnd();
  }

  @Override
  public void writeStructBegin(ai.chronon.api.thrift.protocol.TStruct struct) throws TException {
    writeJSONObjectStart();
  }

  @Override
  public void writeStructEnd() throws TException {
    writeJSONObjectEnd();
  }

  @Override
  public void writeFieldBegin(ai.chronon.api.thrift.protocol.TField field) throws TException {
    if (fieldNamesAsString_) {
      writeString(field.name);
    } else {
      writeJSONInteger(field.id);
    }
    writeJSONObjectStart();
    writeJSONString(getTypeNameForTypeID(field.type));
  }

  @Override
  public void writeFieldEnd() throws TException {
    writeJSONObjectEnd();
  }

  @Override
  public void writeFieldStop() {}

  @Override
  public void writeMapBegin(ai.chronon.api.thrift.protocol.TMap map) throws TException {
    writeJSONArrayStart();
    writeJSONString(getTypeNameForTypeID(map.keyType));
    writeJSONString(getTypeNameForTypeID(map.valueType));
    writeJSONInteger(map.size);
    writeJSONObjectStart();
  }

  @Override
  public void writeMapEnd() throws TException {
    writeJSONObjectEnd();
    writeJSONArrayEnd();
  }

  @Override
  public void writeListBegin(ai.chronon.api.thrift.protocol.TList list) throws TException {
    writeJSONArrayStart();
    writeJSONString(getTypeNameForTypeID(list.elemType));
    writeJSONInteger(list.size);
  }

  @Override
  public void writeListEnd() throws TException {
    writeJSONArrayEnd();
  }

  @Override
  public void writeSetBegin(ai.chronon.api.thrift.protocol.TSet set) throws TException {
    writeJSONArrayStart();
    writeJSONString(getTypeNameForTypeID(set.elemType));
    writeJSONInteger(set.size);
  }

  @Override
  public void writeSetEnd() throws TException {
    writeJSONArrayEnd();
  }

  @Override
  public void writeBool(boolean b) throws TException {
    writeJSONInteger(b ? (long) 1 : (long) 0);
  }

  @Override
  public void writeByte(byte b) throws TException {
    writeJSONInteger(b);
  }

  @Override
  public void writeI16(short i16) throws TException {
    writeJSONInteger(i16);
  }

  @Override
  public void writeI32(int i32) throws TException {
    writeJSONInteger(i32);
  }

  @Override
  public void writeI64(long i64) throws TException {
    writeJSONInteger(i64);
  }

  @Override
  public void writeUuid(UUID uuid) throws TException {
    writeJSONString(uuid.toString().getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void writeDouble(double dub) throws TException {
    writeJSONDouble(dub);
  }

  @Override
  public void writeString(String str) throws TException {
    byte[] b = str.getBytes(StandardCharsets.UTF_8);
    writeJSONString(b);
  }

  @Override
  public void writeBinary(ByteBuffer bin) throws TException {
    writeJSONBase64(
        bin.array(),
        bin.position() + bin.arrayOffset(),
        bin.limit() - bin.position() - bin.arrayOffset());
  }

  /** Reading methods. */

  // Read in a JSON string, unescaping as appropriate.. Skip reading from the
  // context if skipContext is true.
  private TByteArrayOutputStream readJSONString(boolean skipContext) throws TException {
    TByteArrayOutputStream arr = new TByteArrayOutputStream(DEF_STRING_SIZE);
    ArrayList<Character> codeunits = new ArrayList<Character>();
    if (!skipContext) {
      context_.read();
    }
    readJSONSyntaxChar(QUOTE);
    while (true) {
      byte ch = reader_.read();
      if (ch == QUOTE[0]) {
        break;
      }
      if (ch == ESCSEQ[0]) {
        ch = reader_.read();
        if (ch == ESCSEQ[1]) {
          trans_.readAll(tmpbuf_, 0, 4);
          short cu =
              (short)
                  (((short) hexVal(tmpbuf_[0]) << 12)
                      + ((short) hexVal(tmpbuf_[1]) << 8)
                      + ((short) hexVal(tmpbuf_[2]) << 4)
                      + (short) hexVal(tmpbuf_[3]));
          try {
            if (Character.isHighSurrogate((char) cu)) {
              if (codeunits.size() > 0) {
                throw new TProtocolException(
                    TProtocolException.INVALID_DATA, "Expected low surrogate char");
              }
              codeunits.add((char) cu);
            } else if (Character.isLowSurrogate((char) cu)) {
              if (codeunits.size() == 0) {
                throw new TProtocolException(
                    TProtocolException.INVALID_DATA, "Expected high surrogate char");
              }

              codeunits.add((char) cu);
              arr.write(
                  (new String(new int[] {codeunits.get(0), codeunits.get(1)}, 0, 2))
                      .getBytes(StandardCharsets.UTF_8));
              codeunits.clear();
            } else {
              arr.write((new String(new int[] {cu}, 0, 1)).getBytes(StandardCharsets.UTF_8));
            }
            continue;
          } catch (IOException ex) {
            throw new TProtocolException(
                TProtocolException.INVALID_DATA, "Invalid unicode sequence");
          }
        } else {
          int off = ESCAPE_CHARS.indexOf(ch);
          if (off == -1) {
            throw new TProtocolException(TProtocolException.INVALID_DATA, "Expected control char");
          }
          ch = ESCAPE_CHAR_VALS[off];
        }
      }
      arr.write(ch);
    }
    return arr;
  }

  // Return true if the given byte could be a valid part of a JSON number.
  private boolean isJSONNumeric(byte b) {
    switch (b) {
      case '+':
      case '-':
      case '.':
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
      case 'E':
      case 'e':
        return true;
    }
    return false;
  }

  // Read in a sequence of characters that are all valid in JSON numbers. Does
  // not do a complete regex check to validate that this is actually a number.
  private String readJSONNumericChars() throws TException {
    StringBuilder strbld = new StringBuilder();
    while (true) {
      byte ch = reader_.peek();
      if (!isJSONNumeric(ch)) {
        break;
      }
      strbld.append((char) reader_.read());
    }
    return strbld.toString();
  }

  // Read in a JSON number. If the context dictates, read in enclosing quotes.
  private long readJSONInteger() throws TException {
    context_.read();
    if (context_.escapeNum()) {
      readJSONSyntaxChar(QUOTE);
    }
    String str = readJSONNumericChars();
    if (context_.escapeNum()) {
      readJSONSyntaxChar(QUOTE);
    }
    try {
      return Long.parseLong(str);
    } catch (NumberFormatException ex) {
      throw new TProtocolException(
          TProtocolException.INVALID_DATA, "Bad data encounted in numeric data");
    }
  }

  // Read in a JSON double value. Throw if the value is not wrapped in quotes
  // when expected or if wrapped in quotes when not expected.
  private double readJSONDouble() throws TException {
    context_.read();
    if (reader_.peek() == QUOTE[0]) {
      TByteArrayOutputStream arr = readJSONString(true);
      double dub = Double.parseDouble(arr.toString(StandardCharsets.UTF_8));
      if (!context_.escapeNum() && !Double.isNaN(dub) && !Double.isInfinite(dub)) {
        // Throw exception -- we should not be in a string in this case
        throw new TProtocolException(
            TProtocolException.INVALID_DATA, "Numeric data unexpectedly quoted");
      }
      return dub;
    } else {
      if (context_.escapeNum()) {
        // This will throw - we should have had a quote if escapeNum == true
        readJSONSyntaxChar(QUOTE);
      }
      try {
        return Double.parseDouble(readJSONNumericChars());
      } catch (NumberFormatException ex) {
        throw new TProtocolException(
            TProtocolException.INVALID_DATA, "Bad data encounted in numeric data");
      }
    }
  }

  // Read in a JSON string containing base-64 encoded data and decode it.
  private byte[] readJSONBase64() throws TException {
    TByteArrayOutputStream arr = readJSONString(false);
    byte[] b = arr.get();
    int len = arr.len();
    int off = 0;
    int size = 0;
    // Ignore padding
    int bound = len >= 2 ? len - 2 : 0;
    for (int i = len - 1; i >= bound && b[i] == '='; --i) {
      --len;
    }
    while (len >= 4) {
      // Decode 4 bytes at a time
      ai.chronon.api.thrift.protocol.TBase64Utils.decode(b, off, 4, b, size); // NB: decoded in place
      off += 4;
      len -= 4;
      size += 3;
    }
    // Don't decode if we hit the end or got a single leftover byte (invalid
    // base64 but legal for skip of regular string type)
    if (len > 1) {
      // Decode remainder
      ai.chronon.api.thrift.protocol.TBase64Utils.decode(b, off, len, b, size); // NB: decoded in place
      size += len - 1;
    }
    // Sadly we must copy the byte[] (any way around this?)
    byte[] result = new byte[size];
    System.arraycopy(b, 0, result, 0, size);
    return result;
  }

  private void readJSONObjectStart() throws TException {
    context_.read();
    readJSONSyntaxChar(LBRACE);
    pushContext(new JSONPairContext());
  }

  private void readJSONObjectEnd() throws TException {
    readJSONSyntaxChar(RBRACE);
    popContext();
  }

  private void readJSONArrayStart() throws TException {
    context_.read();
    readJSONSyntaxChar(LBRACKET);
    pushContext(new JSONListContext());
  }

  private void readJSONArrayEnd() throws TException {
    readJSONSyntaxChar(RBRACKET);
    popContext();
  }

  @Override
  public ai.chronon.api.thrift.protocol.TMessage readMessageBegin() throws TException {
    resetContext(); // THRIFT-3743
    readJSONArrayStart();
    if (readJSONInteger() != VERSION) {
      throw new TProtocolException(
          TProtocolException.BAD_VERSION, "Message contained bad version.");
    }
    String name = readJSONString(false).toString(StandardCharsets.UTF_8);
    byte type = (byte) readJSONInteger();
    int seqid = (int) readJSONInteger();
    return new TMessage(name, type, seqid);
  }

  @Override
  public void readMessageEnd() throws TException {
    readJSONArrayEnd();
  }

  @Override
  public TStruct readStructBegin() throws TException {
    readJSONObjectStart();
    return ANONYMOUS_STRUCT;
  }

  @Override
  public void readStructEnd() throws TException {
    readJSONObjectEnd();
  }

  @Override
  public ai.chronon.api.thrift.protocol.TField readFieldBegin() throws TException {
    byte ch = reader_.peek();
    byte type;
    short id = 0;
    if (ch == RBRACE[0]) {
      type = TType.STOP;
    } else {
      id = (short) readJSONInteger();
      readJSONObjectStart();
      type = getTypeIDForTypeName(readJSONString(false).get());
    }
    return new TField("", type, id);
  }

  @Override
  public void readFieldEnd() throws TException {
    readJSONObjectEnd();
  }

  @Override
  public ai.chronon.api.thrift.protocol.TMap readMapBegin() throws TException {
    readJSONArrayStart();
    byte keyType = getTypeIDForTypeName(readJSONString(false).get());
    byte valueType = getTypeIDForTypeName(readJSONString(false).get());
    int size = (int) readJSONInteger();
    readJSONObjectStart();
    ai.chronon.api.thrift.protocol.TMap map = new TMap(keyType, valueType, size);

    checkReadBytesAvailable(map);
    return map;
  }

  @Override
  public void readMapEnd() throws TException {
    readJSONObjectEnd();
    readJSONArrayEnd();
  }

  @Override
  public ai.chronon.api.thrift.protocol.TList readListBegin() throws TException {
    readJSONArrayStart();
    byte elemType = getTypeIDForTypeName(readJSONString(false).get());
    int size = (int) readJSONInteger();
    ai.chronon.api.thrift.protocol.TList list = new TList(elemType, size);

    checkReadBytesAvailable(list);
    return list;
  }

  @Override
  public void readListEnd() throws TException {
    readJSONArrayEnd();
  }

  @Override
  public ai.chronon.api.thrift.protocol.TSet readSetBegin() throws TException {
    readJSONArrayStart();
    byte elemType = getTypeIDForTypeName(readJSONString(false).get());
    int size = (int) readJSONInteger();
    ai.chronon.api.thrift.protocol.TSet set = new TSet(elemType, size);

    checkReadBytesAvailable(set);
    return set;
  }

  @Override
  public void readSetEnd() throws TException {
    readJSONArrayEnd();
  }

  @Override
  public boolean readBool() throws TException {
    return (readJSONInteger() != 0);
  }

  @Override
  public byte readByte() throws TException {
    return (byte) readJSONInteger();
  }

  @Override
  public short readI16() throws TException {
    return (short) readJSONInteger();
  }

  @Override
  public int readI32() throws TException {
    return (int) readJSONInteger();
  }

  @Override
  public long readI64() throws TException {
    return readJSONInteger();
  }

  @Override
  public UUID readUuid() throws TException {
    return UUID.fromString(readString());
  }

  @Override
  public double readDouble() throws TException {
    return readJSONDouble();
  }

  @Override
  public String readString() throws TException {
    return readJSONString(false).toString(StandardCharsets.UTF_8);
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    return ByteBuffer.wrap(readJSONBase64());
  }

  /** Return the minimum number of bytes a type will consume on the wire */
  @Override
  public int getMinSerializedSize(byte type) throws TTransportException {
    switch (type) {
      case 0:
        return 0; // Stop
      case 1:
        return 0; // Void
      case 2:
        return 1; // Bool
      case 3:
        return 1; // Byte
      case 4:
        return 1; // Double
      case 6:
        return 1; // I16
      case 8:
        return 1; // I32
      case 10:
        return 1; // I64
      case 11:
        return 2; // string length
      case 12:
        return 2; // empty struct
      case 13:
        return 2; // element count Map
      case 14:
        return 2; // element count Set
      case 15:
        return 2; // element count List
      default:
        throw new TTransportException(TTransportException.UNKNOWN, "unrecognized type code");
    }
  }
}
