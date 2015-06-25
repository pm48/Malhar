/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchbase;

import java.util.Map;

public class TestComplexPojoInput
{
  // Fields are configurable by user using map function.
  private String key;
  private String name;
  private int age;
  private Map<String, Integer> map;

  public String getKey()
  {
    return key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public Map<String, Integer> getMap()
  {
    return map;
  }

  public void setMap(Map<String, Integer> map)
  {
    this.map = map;
  }

  TestComplexPojoInput()
  {

  }

}
