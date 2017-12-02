package com.greentickets.util;

import java.sql.Connection;
import java.sql.Statement;

public class Query {


    private static final String q_Import = "LOAD DATA LOCAL INFILE '" + AppConfig.sourceFileTxt + "\\\\%s.txt' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\\\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES ";
    private static final String q_DataImport_Agency = "(agency_id,agency_name,agency_url,agency_timezone,agency_phone,agency_lang)";
    private static final String q_DataImport_Calendar = "(service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date)";
    private static final String q_DataImport_CalendarDates = "(service_id,date,exception_type)";
    private static final String q_DataImport_FareAttributes = "(fare_id,price,currency_type,payment_method,transfers,agency_id)";
    private static final String q_DataImport_FareRules = "(fare_id,route_id,origin_id,destination_id)";
    private static final String q_DataImport_FeedInfo = "(feed_publisher_name,feed_publisher_url,feed_lang)";
    private static final String q_DataImport_Routes = "(route_id,agency_id,route_short_name,route_long_name,route_type,route_color,competent_authority)";


    private static final String q_createAgency = "CREATE TABLE `agency`(`id` BIGINT(20)  NOT NULL AUTO_INCREMENT, `agency_id`  BIGINT(20) NULL, `agency_name` VARCHAR(500) NULL, `agency_url`  VARCHAR(500) NULL, `agency_timezone` VARCHAR(40) NULL, `agency_phone`  VARCHAR(40) NULL, `agency_lang` VARCHAR(20) NULL, PRIMARY KEY (`id`));";
    private static final String q_createCalendar = "CREATE TABLE `calendar` (`id` BIGINT(20)  NOT NULL AUTO_INCREMENT, `service_id`  BIGINT(20) NULL, `monday` BIT NULL, `tuesday`  BIT(1) NULL, `wednesday` BIT(1) NULL, `thursday`  BIT(1) NULL, `friday` BIT(1) NULL, `saturday` BIT(1) NULL, `sunday` BIT(1) NULL, `start_date` DATE NULL, `end_date` DATE NULL, PRIMARY KEY (`id`));";
    private static final String q_createCalendarDates = "CREATE TABLE `calendar_dates` (`id` BIGINT(20)  NOT NULL AUTO_INCREMENT, `service_id`  BIGINT(20) NULL, `date` DATE NULL, `exception_type` VARCHAR(20) NULL, PRIMARY KEY (`id`));";
    private static final String q_createFareAttributes = "CREATE TABLE `fare_attributes`(`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`fare_id` BIGINT(20) NULL DEFAULT NULL,`price` DECIMAL(10,4) NULL DEFAULT NULL,`currency_type` VARCHAR(10) NULL DEFAULT NULL,`payment_method` VARCHAR(10) NULL DEFAULT NULL,`transfers` VARCHAR(10) NULL DEFAULT NULL,`agency_id` BIGINT(20) NULL DEFAULT NULL, PRIMARY KEY (`id`));";
    private static final String q_createFareRules = "CREATE TABLE `fare_rules`(`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`fare_id` BIGINT(20) NULL DEFAULT NULL,`route_id` BIGINT(20) NULL DEFAULT NULL,`origin_id` BIGINT(20) NULL DEFAULT NULL,`destination_id` BIGINT(20) NULL DEFAULT NULL, PRIMARY KEY (`id`));";
    private static final String q_createFeedInfo = "CREATE TABLE `feed_info`(`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`feed_publisher_name` VARCHAR(200) NULL DEFAULT NULL,`feed_publisher_url` VARCHAR(500) NULL DEFAULT NULL,`feed_lang` VARCHAR(50) NULL DEFAULT NULL, PRIMARY KEY (`id`));";
    private static final String q_createRoutes = "CREATE TABLE `routes`(`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`route_id` BIGINT(20) NULL DEFAULT NULL,`agency_id` BIGINT(20) NULL DEFAULT NULL,`route_short_name` VARCHAR(50) NULL DEFAULT NULL,`route_long_name` VARCHAR(50) NULL DEFAULT NULL,`route_type` VARCHAR(50) NULL DEFAULT NULL,`route_color` VARCHAR(50) NULL DEFAULT NULL,`competent_authority` VARCHAR(200) NULL DEFAULT NULL, PRIMARY KEY (`id`));";

    public static void CSVtoSQL(String tableName, Connection conn) {

        try {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS " + tableName + ";");

            if (tableName == "agency") {
                stmt.execute(q_createAgency);

                stmt.execute(String.format(q_Import, tableName, tableName) + q_DataImport_Agency);
            }
            else if (tableName == "calendar") {
                stmt.execute(q_createCalendar);

                stmt.execute(String.format(q_Import, tableName, tableName) + q_DataImport_Calendar);
            }
            else if (tableName == "calendar_dates") {
                stmt.execute(q_createCalendarDates);

                stmt.execute(String.format(q_Import, tableName, tableName) + q_DataImport_CalendarDates);
            }
            else if (tableName == "fare_attributes") {
                stmt.execute(q_createFareAttributes);

                stmt.execute(String.format(q_Import, tableName, tableName) + q_DataImport_FareAttributes);
            }
            else if (tableName == "fare_rules") {
                stmt.execute(q_createFareRules);

                stmt.execute(String.format(q_Import, tableName, tableName) + q_DataImport_FareRules);
            }
            else if (tableName == "feed_info") {
                stmt.execute(q_createFeedInfo);

                stmt.execute(String.format(q_Import, tableName, tableName) + q_DataImport_FeedInfo);
            }
            else if (tableName == "routes") {
                stmt.execute(q_createRoutes);

                stmt.execute(String.format(q_Import, tableName, tableName) + q_DataImport_Routes);
            }

            System.out.println(tableName + " imported successfully!");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
