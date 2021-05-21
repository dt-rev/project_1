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
            input match {
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("1") => {
                    println("SCENARIO 1:")
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("2") => {
                    println("SCENARIO 2:")
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("3") => {
                    println("SCENARIO 3:")
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("4") => {
                    println("SCENARIO 4:")
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("5") => {
                    println("SCENARIO 5:")
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("6") => {
                    println("SCENARIO 6:")
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
            "----\n"
        ).foreach(println)
    }

}