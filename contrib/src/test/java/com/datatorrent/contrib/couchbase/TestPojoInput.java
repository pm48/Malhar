/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.couchbase;

public class TestPojoInput
{
  private String name;
  private Integer id;
  private Address address;


  public Integer getId()
  {
    return id;
  }

  public void setId(Integer id)
  {
    this.id = id;
  }

  public Address getAddress()
  {
    return address;
  }

  public void setAddress(Address address)
  {
    this.address = address;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  protected static class Address
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
