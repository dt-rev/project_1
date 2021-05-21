package com.revature.proj0.utils

import java.sql.Connection
import java.sql.DriverManager

object ConnectionUtil {
    var conn: Connection = null

    def getConnection(db: String, user: String, pw: String): Connection = {
        if (conn == null || conn.isClosed()) {
            classOf[org.apache.hive.jdbc.HiveDriver].newInstance()

            conn = DriverManager.getConnection(db, user, pw)
        }

        return conn
    }
}