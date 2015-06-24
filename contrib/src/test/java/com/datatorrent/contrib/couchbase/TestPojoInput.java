/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.couchbase;

public class TestPojoInput
{
  private String key;
  private Integer age;

  public Integer getAge()
  {
    return age;
  }

  public void setAge(Integer age)
  {
    this.age = age;
  }
  private Address address;

  public String getKey()
  {
    return key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public Address getAddress()
  {
    return address;
  }

  public void setAddress(Address address)
  {
    this.address = address;
  }


  public static class Address
  {
    private int housenumber;

    public int getHousenumber()
    {
      return housenumber;
    }

    public void setHousenumber(int housenumber)
    {
      this.housenumber = housenumber;
    }

    public String getCity()
    {
      return city;
    }

    public void setCity(String city)
    {
      this.city = city;
    }
    private String city;
    public Address()
    {
    }

  }

}
