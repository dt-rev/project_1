package com.revature.proj0.cli

import scala.util.matching.Regex
import scala.io.StdIn
import com.revature.proj0.utils.ConnectionUtil
import com.revature.proj0.dao.Dao

class Cli {
    val commandArgPattern: Regex = "(\\w+)\\s*(.*)".r

    val db   = "jdbc:hive2://localhost:10000/project1"
    val user = ""
    val pass = ""

    def menu(): Unit = {
        printWelcome()

        var continueMenuLoop = true

        do {
            printMenuOptions()

            var input = StdIn.readLine()
            println()
            input match {
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("1") => {
                    println("SCENARIO 1:")

                    println("\nWhat is the total number of consumers for Branch1?")
                    println("What is the number of consumers for the Branch2?")
                    println("select branch.branch BRANCH, sum(conscount.count) CONSUMERS")
                    println("from branch left join conscount")
                    println("on branch.beverage = conscount.beverage")
                    println("group by branch.branch")
                    println("having branch.branch = 'Branch1' or branch.branch = 'Branch2';")

                    val conn = ConnectionUtil.getConnection(db, user, pass)
                    val rs = Dao.scen1(conn)

                    println("\nBRANCH  CONSUMERS")
                    while(rs.next()) {
                        println(f"${rs.getString("branch")}%-7s ${rs.getString("consumers")}")
                    }
                    
                    conn.close
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("2") => {
                    println("SCENARIO 2:")

                    println("\nWhat is the most consumed beverage on Branch1?")
                    println("select b.beverage Branch1_Beverage, sum(c.count) count")
                    println("from branch as b left join conscount as c")
                    println("on b.beverage = c.beverage")
                    println("where b.branch = 'Branch1'")
                    println("group by b.beverage")
                    println("order by count desc")
                    println("limit 1;")

                    val conn = ConnectionUtil.getConnection(db, user, pass)

                    println("\nWhat is the least consumed beverage on Branch2?")
                    println("select b.beverage Branch1_Beverage, sum(c.count) count")
                    println("from branch as b left join conscount as c")
                    println("on b.beverage = c.beverage")
                    println("where b.branch = 'Branch2'\"'")
                    println("group by b.beverage")
                    println("order by count asc")
                    println("limit 1;")

                    
                    
                    conn.close
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("3") => {
                    println("SCENARIO 3:")

                    println("\nWhat are the beverages available on Branch10, Branch8, and Branch1?")
                    println("select b.branch as branch, beverage from branch b where b.branch = 'Branch10'")
                    println("or branch = 'Branch8'")
                    println("or branch = 'Branch1';")

                    val conn = ConnectionUtil.getConnection(db, user, pass)
                    
                    println("\nWhat are the common beverages available in Branch4, Branch7?")
                    println("select distinct beverage common_beverages from branch where branch = 'Branch4'")
                    println("intersect select distinct beverage common_beverages from branch where branch = 'Branch7';")


                    
                    conn.close
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("4") => {
                    println("SCENARIO 4:")

                    println("\nCreate a view for Scenario 3.")
                    println("create view if not exists s4_view as")
                    println("select b.branch as branch, beverage from branch b where b.branch = 'Branch10'")
                    println("or branch = 'Branch8'")
                    println("or branch = 'Branch1';")

                    val conn = ConnectionUtil.getConnection(db, user, pass)

                    println("\nCreate a partition for Scenario 3.")
                    println("create table if not exists s4_partition(beverage String) partitioned by(branch String);")
                    println("set hive.exec.dynamic.partition.mode=nonstrict;")
                    println("insert overwrite table s4_partition partition(branch)")
                    println("select beverage, branch from s4_view;")


                    
                    println("\nCreate a index for Scenario 3.")
                    println("drop index if exists s4_index on s4_partition;")
                    println("create index s4_index on table s4_partition(beverage)")
                    println("as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'")
                    println("with deferred rebuild;")
                    println("alter index s4_index on s4_partition rebuild;")
                    println("show indexes on s4_partition;")



                    conn.close
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("5") => {
                    println("SCENARIO 5:")

                    println("\nAlter the table properties to add \"note\", \"comment\".")
                    println("alter table s4_partition set tblproperties(\"note\"=\"this partitioned table shows the beverages available at branches 1, 8, and 10\");")
                    println("alter table s4_partition set tblproperties(\"comment\"=\"Wow! What a great table!\");")

                    val conn = ConnectionUtil.getConnection(db, user, pass)
                    conn.close
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("6") => {
                    println("SCENARIO 6:")

                    println("\nRemove the fifth row from the output of Scenario 1.")

                    println("create table if not exists s6_table(row_num Int, branch String, consumers Int);")

                    println("insert overwrite table s6_table")
                    println("select ROW_NUMBER() over (), branch.branch BRANCH, sum(conscount.count) CONSUMERS")
                    println("from branch left join conscount")
                    println("on branch.beverage = conscount.beverage")
                    println("group by branch.branch;")
                    
                    println("insert overwrite table s6_table")
                    println("select * from s6_table")
                    println("where row_num <> 5;")

                    val conn = ConnectionUtil.getConnection(db, user, pass)
                    conn.close
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("9") => {
                    val conn = ConnectionUtil.getConnection(db, user, pass)
                    val rs = Dao.test(conn)

                    println("\nTESTING DB CONNECTION: LISTING DATABASES")
                    while(rs.next()) {
                        println(rs.getString("database_name"))
                    }
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("0") => {
                    continueMenuLoop = false
                }
                
                case _ => {
                    println("INVALID OPTION")
                }
            }

        } while (continueMenuLoop)
    }

    def printWelcome(): Unit = {
        println("\nQUERYING SAMPLE BEVERAGE SALES")
    }
    
    def printMenuOptions(): Unit = {
        List(
            "\n----\nMENU\n----",
            "[1] Scenario 1",
            "[2] Scenario 2",
            "[3] Scenario 3",
            "[4] Scenario 4",
            "[5] Scenario 5",
            "[6] Scenario 6",
            //"[9] Test",
            "[0] Quit",
            "----"
        ).foreach(println)
    }

}