package com.hadapt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public class PostgresWorkerPool {
    static final int DEFAULT_NUM_WORKERS = 4;

    static PostgresWorkerPool __poolInstance = null;

    static boolean __configured = false;
    static String __url;
    static Properties __props;
    static int __numWorkers;
    static ArrayList<Connection> __connections;

    public static PostgresWorkerPool getInstance() throws SQLException, IllegalStateException {
        if (!__configured) {
            throw new IllegalStateException("PostgresWorkerPool: must configure first");
        }
        if (__poolInstance == null) {
            __poolInstance = new PostgresWorkerPool(__url, __props, __numWorkers);
        }
        return __poolInstance;
    }

    public static void configure(String url, Properties props) {
        configure(url, props, DEFAULT_NUM_WORKERS);
    }

    public static void configure(String url, Properties props, int numWorkers) {
        __url = url;
        __props = props;
        __numWorkers = numWorkers;
        __configured = true;
    }

    private PostgresWorkerPool(String url, Properties props, int numWorkers) throws SQLException {
        __connections = new ArrayList<Connection>();
        for (int i = 0; i < __numWorkers; i++) {
            __connections.add(DriverManager.getConnection(url, props));
        }
    }

    public PostgresWorker getWorker() {
        // FIXME: What if no connections???
        return new PostgresWorker(__connections.remove(0));
    }

    public void returnWorker(PostgresWorker worker) {
        __connections.add(worker.getConnection());
    }

    public void shutdown() {
        for (Connection c : __connections) {
            try {
                c.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
