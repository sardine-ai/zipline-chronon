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
import ai.chronon.api.thrift.protocol.TProtocol;

/** Generic base interface for generated Thrift objects. */
public interface TSerializable {

  /**
   * Reads the TObject from the given input protocol.
   *
   * @param iprot Input protocol
   * @throws ai.chronon.api.thrift.TException if there is an error reading from iprot
   */
  void read(TProtocol iprot) throws ai.chronon.api.thrift.TException;

  /**
   * Writes the objects out to the protocol
   *
   * @param oprot Output protocol
   * @throws ai.chronon.api.thrift.TException if there is an error writing to oprot
   */
  void write(TProtocol oprot) throws TException;
}
