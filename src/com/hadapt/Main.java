package com.hadapt;

import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;

import com.hadapt.catalog.CatalogService;


// http://jdbc.postgresql.org/documentation/head/load.html
public class Main {
    static final String POSTGRES_JDBC_BASE_URL = "jdbc:postgresql:";
    static final String CREATE_DOCUMENT_SCHEMA_STATEMEMT =
            "CREATE TABLE IF NOT EXISTS information_schema.documents(" +
                    "key_name text, table_name text, column_name text," +
                    "key_type text, materialized boolean, clean boolean, count int," +
                    "UNIQUE (key_name, table_name, key_type))";

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

        try {
            Class.forName("org.postgresql.Driver"); // Initialize JDBC to use postgresql extensions
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        PostgresWorkerPool.initialize(url, props);
        PostgresWorker worker = PostgresWorkerPool.getInstance().getWorker();

        try {
            while (true) {
                String statement = sc.nextLine();
                analyze_execute(statement);
            }
        } catch (NoSuchElementException e) {
            System.err.println("Shutting down connection");
            PostgresWorkerPool.getInstance().returnWorker(worker);
            PostgresWorkerPool.getInstance().shutdown();
            return;
        }
    }

    public void analyze_execute(String stmt) {
        // Basic parse; operations; delegate if necessary; basic execution

    }
}
