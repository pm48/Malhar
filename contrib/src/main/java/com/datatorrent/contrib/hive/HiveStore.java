/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hive;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveStore extends JdbcStore
{
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final Logger logger = LoggerFactory.getLogger(HiveStore.class);

  //protected HiveStore store = new HiveStore();
  public HiveStore()
  {
    super();
    this.setDbDriver(driverName);
  }





  /*public HiveStore getHiveStore(){
   return store;
   }

   public void setHiveStore(HiveStore store){
   this.store = store;
   }
   /* public String getHost()
   {
   return host;
   }

   public Connection getConnection(){
   return connection;
   }

   public void setHost(String host)
   {
   this.host = host;
   }

   public String getUser()
   {
   return user;
   }

   public void setUser(String user)
   {
   this.user = user;
   }


   public int getPort()
   {
   return port;
   }

   public void setPort(int port)
   {
   this.port = port;
   }


   @Override
   public void connect()
   {
   super.connect();
   }


   @Override
   public void disconnect()
   {
   try {
   connection.close();
   }
   catch (SQLException ex) {
   throw new RuntimeException("Error while closing database resource", ex);
   }
   }

   @Override
   public boolean connected()
   {
   try {
   return !connection.isClosed();
   }
   catch (SQLException e) {
   throw new RuntimeException(e);
   }
   }*/

}
