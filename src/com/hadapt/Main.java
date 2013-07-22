package com.hadapt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadapt.loader.JsonLoader;
import com.hadapt.upgrader.ColumnUpgrader;

// http://jdbc.postgresql.org/documentation/head/load.html
public class Main {
    static final String POSTGRES_JDBC_BASE_URL = "jdbc:postgresql:";
    static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        System.out.print("Enter database name: ");
        String dbName = "test" ;//sc.nextLine();
        System.out.print("Enter user: ");
        String user = "postgres";//sc.nextLine();
        System.out.print("Enter password: ");
        String pwd = "";//sc.nextLine();
        // TODO get command line input for properties
        Properties props = new Properties();
        String url = POSTGRES_JDBC_BASE_URL + dbName;
        props.setProperty("user", user);
        props.setProperty("password", pwd);

        PostgresWorker worker = null;
        try {
            Class.forName("org.postgresql.Driver"); // Initialize JDBC to use postgresql extensions
            PostgresWorkerPool.configure(url, props);
            worker = PostgresWorkerPool.getInstance().getWorker();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        logger.info("Successfully authenticated to DB {} with username {}", dbName, user);
        // Main REPL loop
        try {
            while (true) {
                String statement = sc.nextLine();
                System.out.println(statement);
                analyze_execute(statement);
            }
        } catch (NoSuchElementException e) {
            logger.info("No more commands. Shutting down DB connections");
        } finally {
            try {
                PostgresWorkerPool.getInstance().returnWorker(worker);
                PostgresWorkerPool.getInstance().shutdown();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return;
        }
    }

    public static void analyze_execute(String stmt) {
        String[] commandToks = stmt.split(" ");
        if (commandToks.length <= 0) {
            return;
        }

        if (commandToks[0].equalsIgnoreCase("load")) {
            if (commandToks.length < 4 || !commandToks[2].equalsIgnoreCase(("from"))) {
                System.out.println("LOAD command takes the form 'LOAD table FROM file'");
                return;
            }
            String tname = commandToks[1];
            String dataPath = commandToks[3];
            JsonLoader loader = new JsonLoader(tname, dataPath);
            try {
                loader.loadTable(new FileWriter(File.createTempFile("load", ".rejects")));
                ColumnUpgrader upgrader = new ColumnUpgrader(tname);
                upgrader.execute();
            } catch (SQLException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        } else {


        }
    }
}
