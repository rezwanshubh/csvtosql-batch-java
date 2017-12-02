package com.greentickets;

import com.greentickets.util.AppConfig;
import com.greentickets.util.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@SpringBootApplication
public class Application {

    @Autowired
    public DataSource dataSource;

    public static void main(String[] args) throws Exception {

        Connection con = null;
        PreparedStatement prepStmt = null;
        try {

            // registering Oracle driver class
            Class.forName(AppConfig.driverClassName);

            // getting connection
            con = DriverManager.getConnection(
                    AppConfig.url,
                    AppConfig.username, AppConfig.password);
            System.out.println("Connection established successfully!");

            con.setAutoCommit(false); //Now, transactions won't be committed automatically.

            //Query.CSVtoSQL("agency", con);
            //Query.CSVtoSQL("calendar", con);
            //Query.CSVtoSQL("calendar_dates", con);
            //Query.CSVtoSQL("fare_attributes", con);
            //Query.CSVtoSQL("fare_rules", con);
            //Query.CSVtoSQL("feed_info", con);
            //Query.CSVtoSQL("routes", con);
            /*


            Query.CSVtoSQL("routes", con);
            Query.CSVtoSQL("shapes", con);
            Query.CSVtoSQL("stop_times", con);
            Query.CSVtoSQL("stops", con);
            Query.CSVtoSQL("trips", con);
*/
            con.commit(); //commit all the transactions


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (prepStmt != null) prepStmt.close(); //close PreparedStatement
                if (con != null) con.close(); // close connection
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
