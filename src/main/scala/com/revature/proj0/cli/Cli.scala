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
        println("Welcome to Duncan Thomas's Revature Project 1!")

        var continueMenuLoop = true

        do {
            printMenuOptions()

            var input = StdIn.readLine()
            input match {
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("1") => {
                    val conn = ConnectionUtil.getConnection(db, user, pass)
                    val rs = Dao.test(conn)

                    while(rs.next()) {
                        println(rs.getString("database_name"))
                    }
                }
                
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("0") => {
                    continueMenuLoop = false
                }
            }

        } while (continueMenuLoop)
    }

    def printMenuOptions(): Unit = {
        List(
            "\n----\nMENU\n----",
            "[1] Test",
            "[0] Quit",
            "----\n"
        ).foreach(println)
    }

}