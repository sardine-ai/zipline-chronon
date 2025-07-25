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

package ai.chronon.api.thrift;

import ai.chronon.api.thrift.TException;
import ai.chronon.api.thrift.TSerializable;
import ai.chronon.api.thrift.protocol.TField;
import ai.chronon.api.thrift.protocol.TProtocol;
import ai.chronon.api.thrift.protocol.TProtocolUtil;
import ai.chronon.api.thrift.protocol.TStruct;
import ai.chronon.api.thrift.protocol.TType;

/** Application level exception */
public class TApplicationException extends ai.chronon.api.thrift.TException implements TSerializable {

  private static final TStruct TAPPLICATION_EXCEPTION_STRUCT = new TStruct("TApplicationException");
  private static final TField MESSAGE_FIELD = new TField("message", TType.STRING, (short) 1);
  private static final TField TYPE_FIELD = new TField("type", TType.I32, (short) 2);

  private static final long serialVersionUID = 1L;

  public static final int UNKNOWN = 0;
  public static final int UNKNOWN_METHOD = 1;
  public static final int INVALID_MESSAGE_TYPE = 2;
  public static final int WRONG_METHOD_NAME = 3;
  public static final int BAD_SEQUENCE_ID = 4;
  public static final int MISSING_RESULT = 5;
  public static final int INTERNAL_ERROR = 6;
  public static final int PROTOCOL_ERROR = 7;
  public static final int INVALID_TRANSFORM = 8;
  public static final int INVALID_PROTOCOL = 9;
  public static final int UNSUPPORTED_CLIENT_TYPE = 10;

  protected int type_ = UNKNOWN;
  private String message_ = null;

  public TApplicationException() {
    super();
  }

  public TApplicationException(int type) {
    super();
    type_ = type;
  }

  public TApplicationException(int type, String message) {
    super(message);
    type_ = type;
  }

  public TApplicationException(String message) {
    super(message);
  }

  public int getType() {
    return type_;
  }

  @Override
  public String getMessage() {
    if (message_ == null) {
      return super.getMessage();
    } else {
      return message_;
    }
  }

  public void read(TProtocol iprot) throws ai.chronon.api.thrift.TException {
    TField field;
    iprot.readStructBegin();

    String message = null;
    int type = UNKNOWN;

    while (true) {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      switch (field.id) {
        case 1:
          if (field.type == TType.STRING) {
            message = iprot.readString();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2:
          if (field.type == TType.I32) {
            type = iprot.readI32();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
    type_ = type;
    message_ = message;
  }

  /**
   * Convenience factory method for constructing a TApplicationException given a TProtocol input
   *
   * @param iprot protocol from which an instance of TApplicationException is read.
   * @return an instance of TApplicationException read from iprot.
   * @throws ai.chronon.api.thrift.TException if there is an error reading from iprot.
   */
  public static TApplicationException readFrom(TProtocol iprot) throws ai.chronon.api.thrift.TException {
    TApplicationException result = new TApplicationException();
    result.read(iprot);
    return result;
  }

  public void write(TProtocol oprot) throws TException {
    oprot.writeStructBegin(TAPPLICATION_EXCEPTION_STRUCT);
    if (getMessage() != null) {
      oprot.writeFieldBegin(MESSAGE_FIELD);
      oprot.writeString(getMessage());
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(TYPE_FIELD);
    oprot.writeI32(type_);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }
}
