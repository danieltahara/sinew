package com.hadapt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hadapt.loader.JsonLoader;


// http://jdbc.postgresql.org/documentation/head/load.html
public class Main {
    static final String POSTGRES_JDBC_BASE_URL = "jdbc:postgresql:";
//                    "key_name text, table_name text, column_name text," +
//                    "key_type text, materialized boolean, clean boolean, count int," +
//                    "UNIQUE (key_name, table_name, key_type))";

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        System.out.print("Enter database name: ");
        String dbName = sc.nextLine();
        System.out.print("Enter user: ");
        String user = sc.nextLine();
        System.out.print("Enter password: ");
        String pwd = sc.nextLine();
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

        // Main REPL loop
        try {
            while (true) {
                String statement = sc.nextLine();
                analyze_execute(statement);
            }
        } catch (NoSuchElementException e) {
            System.err.println("Shutting down connection");
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
        Pattern loadPattern = Pattern.compile("^\\s+LOAD\\s+TABLE\\s+(\\w+)\\s+FROM(\\w+)\\s+$");
        Matcher matcher = loadPattern.matcher(stmt);
        if (matcher.groupCount() > 0) {
            JsonLoader loader = new JsonLoader(matcher.group(1), matcher.group(2));
            try {
                loader.loadTable(new FileWriter(File.createTempFile("load", "rejects")));
            } catch (SQLException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        } else {


        }
    }
}
