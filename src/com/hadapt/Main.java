package com.hadapt.handler;

import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;

import com.hadapt.handler.catalog.CatalogService;


// http://jdbc.postgresql.org/documentation/head/load.html
public class Main {
    static final String POSTGRES_JDBC_BASE_URL = "jdbc:postgresql:";

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
        props.setProperty("user",user);
        props.setProperty("password",pwd);

        PostgresWorker postgresWorker = null;
        try {
            Class.forName("org.postgresql.Driver"); // Initialize JDBC to use postgresql extensions
            CatalogService.getCatalog().configure(url, props);
            postgresWorker = new PostgresWorker(url, props);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return;
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
            return;
        }

        try {
            while (true) {
                String statement = sc.nextLine();
                postgresWorker.execute(statement);
            }
        } catch (NoSuchElementException e) {
            CatalogService.getCatalog().close();
            return;
        }
    }
}
