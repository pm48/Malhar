/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.couchbase;

public class TestPojoInput
{
  String key;
  TestPojo test;

    public TestPojo getTest()
    {
      return test;
    }

    public void setTest(TestPojo test)
    {
      this.test = test;
    }

    public String getKey()
    {
      return key;
    }

    public void setKey(String key)
    {
      this.key = key;
    }

    TestPojoInput()
    {

    }

}
