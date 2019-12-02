package com.example.demo.clickhouse;

import com.example.demo.models.Type;
import javafx.scene.control.ChoiceDialog;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Component
public class ClickHouseService {

    private final ClickHouseConnection clickHouseConnection;

    public ClickHouseService(ClickHouseConnection clickHouseConnection) {
        this.clickHouseConnection = clickHouseConnection;
    }

    public Set<String> getAllTables() {
        Set<String> dbsList = new LinkedHashSet<>();
        try {

            Connection conn = clickHouseConnection.getConnection();

            Statement stmt = conn.createStatement();
            String sql = "show tables";//
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                dbsList.add(rs.getString(1));
            }
            conn.commit();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            return null;
        }
        return dbsList;
    }

    public void deleteTable(String tableName) throws SQLException {
        Connection connection = clickHouseConnection.getConnection();

        Statement statement = connection.createStatement();
        statement.executeQuery("drop table " + tableName);

        connection.commit();
        statement.close();
    }

    public boolean createTable(String tableName, Map<String, Type> columnNameAndType) {
        try {

            columnNameAndType = convertToClickHouseTypes(columnNameAndType);

            String sql = prepareCreateTableStatement(tableName, columnNameAndType);
            System.out.println(sql);
            Connection connection = clickHouseConnection.getConnection();

            Statement statement = connection.createStatement();
            statement.executeQuery(sql);

            connection.commit();
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String prepareCreateTableStatement(String tableName, Map<String, Type> columnNameAndType) {
        String sql = "create table " + tableName + "(";
        int i = 1;
        for (Map.Entry<String, Type> entry :
                columnNameAndType.entrySet()) {
            if (i == columnNameAndType.size()) {
                sql += "\n`" + entry.getKey() + "` " + entry.getValue().getTypeName();
                break;
            }
            sql += "\n`" + entry.getKey() + "` " + entry.getValue().getTypeName() + ",";
            i++;
        }
        sql += "\n)";

        List<String> cols = new ArrayList<>();

        for (Map.Entry<String, Type> entry :
                columnNameAndType.entrySet()) {
            cols.add(entry.getKey());
        }

        ChoiceDialog<String> dialog1 = new ChoiceDialog<>(cols.get(0), cols);
        dialog1.setTitle("Wybierz pole klucz główny");
        dialog1.setHeaderText("Wybierz pole, które ma zostać kluczem głównym");
        dialog1.setContentText("Wybierz pole");

        Optional<String> id = dialog1.showAndWait();
        if (!id.isPresent()) {
            throw new IllegalArgumentException("Id nie zostało wybrane.");
        }

        List<String> datesList = new ArrayList<>();

        for (Map.Entry<String, Type> entry :
                columnNameAndType.entrySet()) {
            if (entry.getValue().getTypeName().equals("Date")) {
                datesList.add(entry.getKey());
            }
        }

        ChoiceDialog<String> dialog2 = new ChoiceDialog<>(datesList.get(0), datesList);
        dialog2.setTitle("Wybierz pole daty");
        dialog2.setHeaderText("Należy wybrać pole daty do porządkowania rekordów w bazie. Wymagane przez silnik tabeli MergeTree");
        dialog2.setContentText("Wybierz pole");

        Optional<String> date = dialog2.showAndWait();
        if (!date.isPresent()) {
            throw new IllegalArgumentException("Nie wybrano pola daty");
        }

        sql += " ENGINE = MergeTree(" + date.get() + ", (" + id.get() + ", " + date.get() + "), 8192)";


        return sql;
    }

    public Map<String, Type> convertToClickHouseTypes(Map<String, Type> columnNameAndType) {
        for (Map.Entry<String, Type> entry : columnNameAndType.entrySet()) {
            if (entry.getValue().getTypeName().equals("CHAR") && entry.getValue().getTypeSize().equals("1")) {
                columnNameAndType.get(entry.getKey()).setTypeName("Boolean");
                continue;
            }
            switch (entry.getValue().getTypeName()) {
                case "SMALLINT":
                    entry.getValue().setTypeName("Int16");
                    break;
                case "INTEGER":
                    entry.getValue().setTypeName("Int32");
                    break;
                case "BIGINT":
                    entry.getValue().setTypeName("Int64");
                    break;
                case "FLOAT":
                    entry.getValue().setTypeName("Float32");
                    break;
                case "DATE":
                    entry.getValue().setTypeName("Date");
                    break;
                case "TIME":
                case "BLOB":
                case "VARCHAR":
                    entry.getValue().setTypeName("String");
                    break;
                case "CHAR":
                    entry.getValue().setTypeName("FixedString(" + entry.getValue().getTypeSize() + ")");
                    break;
                case "DOUBLE PRECISION":
                    entry.getValue().setTypeName("Float64");
                    break;
                case "TIMESTAMP":
                    entry.getValue().setTypeName("DateTime");
                    break;
                default:
                    throw new IllegalArgumentException("Błąd podczas tłumaczenia typu danych z firebird do Clickhouse");
            }
        }
        return columnNameAndType;
    }

    public boolean insertBatchData(String tableName, int numberOfCols, List<String> data) throws SQLException {
        final String[] query = {"insert into " + tableName + " values ("};
        AtomicInteger i = new AtomicInteger(1);
        data.forEach(value -> {
            if(isNumeric(value)){
               query[0] += value;
            } else {
                query[0] += "'" + value + "'";
            }
            if(i.get() == data.size()){
                query[0] += ")";
            } else {
                query[0] += ", ";
            }
            i.getAndIncrement();
        });
        Statement statement = clickHouseConnection.getStatement();
        statement.closeOnCompletion();
        return statement.execute(query[0]);
    }

    public boolean isNumeric(String strNum) {
        Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");

        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }
}
