package com.hadapt.upgrader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import com.hadapt.PostgresWorker;
import com.hadapt.PostgresWorkerPool;
import com.hadapt.catalog.Attribute;

import sun.plugin.dom.exception.InvalidStateException;

public class ColumnUpgrader {
    public static int MAX_NUM_COLUMNS = 1000;
    public static double MIN_SPARSITY = 0.5;

    private String _relname;

    public ColumnUpgrader(String relname) {
        _relname = relname;
    }

    public void execute() throws SQLException, InvalidStateException {
        execute(MIN_SPARSITY);
    }

    public void execute(double minSparsity) throws SQLException, InvalidStateException {
        HashMap<String, Object> baseData = new HashMap<String, Object>();
        baseData.put("table_name", _relname);
        baseData.put("column_name", _relname);
        baseData.put("materialized", new Boolean(true));

        PostgresWorker worker = PostgresWorkerPool.getInstance().getWorker();
        try {
            ResultSet rsRecordsCount = worker.select("count(*)", _relname);
            int numRecords = 0;
            // TODO: assert length = 1
            while (rsRecordsCount.next()) {
                numRecords = rsRecordsCount.getInt("count");
            }
            int minCount = (int)(minSparsity * numRecords + 0.5);
            if (minCount != 0) {
                ResultSet rsDocInfo = worker.select("*",
                    "document_schema." + _relname,
                    "WHERE materialized = false AND count >= " + minCount);
                ArrayList<Attribute> upgrades = new ArrayList<Attribute>();
                // TODO: worker.startTransaction();
                while (rsDocInfo.next()) {
                    if (!rsDocInfo.getBoolean("materialized")) {
                        rsDocInfo.updateBoolean("materialized", true);
                        rsDocInfo.updateRow();
                        upgrades.add(new Attribute(rsDocInfo.getString("key_name"),
                            rsDocInfo.getString("key_type")));
                    }
                }
                // TODO: worker.endTransaction();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            PostgresWorkerPool.getInstance().returnWorker(worker);
        }
    }

}
