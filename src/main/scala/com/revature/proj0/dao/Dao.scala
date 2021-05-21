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

    def scen1(conn: Connection): ResultSet = {
        val stmt = conn.prepareStatement("select branch.branch BRANCH, sum(conscount.count) CONSUMERS "+
                                            "from branch left join conscount " +
                                            "on branch.beverage = conscount.beverage " +
                                            "group by branch.branch " +
                                            "having branch.branch = 'Branch1' or branch.branch = 'Branch2'")
        
        stmt.execute()
        stmt.getResultSet()
    }
}