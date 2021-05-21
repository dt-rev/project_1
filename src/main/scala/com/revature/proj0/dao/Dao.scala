package com.revature.proj0.dao

import java.sql.Connection
import com.revature.proj0.utils.ConnectionUtil
import java.sql.ResultSet

object Dao {
    def test(conn: Connection): ResultSet = {
        val stmt = conn.prepareStatement("show databases")

        stmt.execute()

        stmt.getResultSet()
    }
}