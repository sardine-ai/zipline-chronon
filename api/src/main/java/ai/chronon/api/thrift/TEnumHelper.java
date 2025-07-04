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

import ai.chronon.api.thrift.TEnum;

import java.lang.reflect.Method;

/** Utility class with static methods for interacting with TEnum */
public class TEnumHelper {

  /* no instantiation */
  private TEnumHelper() {}

  /**
   * Given a TEnum class and integer value, this method will return the associated constant from the
   * given TEnum class. This method MUST be modified should the name of the 'findByValue' method
   * change.
   *
   * @param enumClass TEnum from which to return a matching constant.
   * @param value Value for which to return the constant.
   * @return The constant in 'enumClass' whose value is 'value' or null if something went wrong.
   */
  public static ai.chronon.api.thrift.TEnum getByValue(Class<? extends ai.chronon.api.thrift.TEnum> enumClass, int value) {
    try {
      Method method = enumClass.getMethod("findByValue", int.class);
      return (TEnum) method.invoke(null, value);
    } catch (ReflectiveOperationException nsme) {
      return null;
    }
  }
}
