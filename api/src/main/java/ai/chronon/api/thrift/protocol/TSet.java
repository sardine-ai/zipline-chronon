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

import ai.chronon.api.thrift.protocol.TList;
import ai.chronon.api.thrift.protocol.TType;

/** Helper class that encapsulates set metadata. */
public final class TSet {
  public TSet() {
    this(TType.STOP, 0);
  }

  public TSet(byte t, int s) {
    elemType = t;
    size = s;
  }

  public TSet(TList list) {
    this(list.elemType, list.size);
  }

  public final byte elemType;
  public final int size;

  public byte getElemType() {
    return elemType;
  }

  public int getSize() {
    return size;
  }
}
