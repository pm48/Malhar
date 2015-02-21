package com.datatorrent.demos.dimensions.ads;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

public class HiveMetastoreJDBCTest {

    public static void main(String[] args) throws Exception {

        Connection conn = null;
        try {
            HiveConf conf = new HiveConf();
            conf.addResource(new Path("file:///usr/local/Cellar/hive/0.13.1/libexec/conf/hive-site.xml"));
            Class.forName(conf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
            conn = DriverManager.getConnection(
                    conf.getVar(ConfVars.METASTORECONNECTURLKEY),
                    conf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME),
                    conf.getVar(ConfVars.METASTOREPWD));
           System.out.println(conf.getAllProperties().toString());

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(
                "select t.tbl_name, s.location from tbls t " +
                "join sds s on t.sd_id = s.sd_id");
            while (rs.next()) {
                System.out.println(rs.getString(1) + " : " + rs.getString(2));
            }
        }
        finally {
            if (conn != null) {
                conn.close();
            }
        }

    }
}