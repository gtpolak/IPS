package com.example.demo.clickhouse;

import com.example.demo.models.Type;
import javafx.scene.control.ChoiceDialog;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextInputDialog;
import org.springframework.stereotype.Component;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
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
        dialog1.setTitle("Wybierz pole porządkujące");
        dialog1.setHeaderText("Należy wybrać pole daty do porządkowania rekordów w bazie.");
        dialog1.setContentText("Wybierz pole");

        Optional<String> orderCol = dialog1.showAndWait();
        if (!orderCol.isPresent()) {
            throw new IllegalArgumentException("Pole do porządkowania nie zostało wybrane");
        }

        sql += "ENGINE = MergeTree\n" +
                "ORDER BY " + orderCol.get();


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

    public void insertBatchData(String tableName, Map<String, Type> colsTypes, List<String[]> data) throws SQLException {
        List<String> inserts = new ArrayList<>();
        data.forEach(strings -> {
            inserts.add(createInsertStatement(strings, colsTypes, tableName));
        });
        Statement statement = clickHouseConnection.getStatement();
        for (String insert : inserts) {
            statement.execute(insert);
        }
        statement.close();
    }

    public boolean isNumeric(String strNum) {
        Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");

        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }

    private String createInsertStatement(String[] strings, Map<String, Type> firebirdType, String tableName) {
        AtomicInteger index = new AtomicInteger(0);
        final String[] query = new String[]{"insert into " + tableName + " values ("};
        for (Map.Entry<String, Type> entry : firebirdType.entrySet()) {
            if (entry.getValue().getTypeName().contains("Int") || entry.getValue().getTypeName().contains("Float")) {
                if (index.get() == firebirdType.size() - 1) {
                    query[0] += strings[index.getAndIncrement()] + ")";
                } else {
                    query[0] += strings[index.getAndIncrement()] + ", ";
                }
            } else {
                if (strings[index.get()].contains("'")) {
                    strings[index.get()] = strings[index.get()].replaceAll("'", "''");
                }
                if (entry.getValue().getTypeName().equals("DateTime")) {
                    strings[index.get()] = strings[index.get()].substring(0, 19);
                }
                if (index.get() == firebirdType.size() - 1) {
                    query[0] += "'" + strings[index.getAndIncrement()] + "')";
                } else {
                    query[0] += "'" + strings[index.getAndIncrement()] + "', ";
                }
            }
        }
        return query[0];
    }

    public Map<String, Type> getTableDescription(String tableName) throws SQLException {
        Map<String, Type> tableDesc = new LinkedHashMap<>();
        String query = "desc " + tableName;
        Statement statement = clickHouseConnection.getStatement();
        statement.closeOnCompletion();
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            tableDesc.put(resultSet.getString(1), new Type(resultSet.getString(2)));
        }
        return tableDesc;
    }

    public int getTableCount(String tableName) throws SQLException, NumberFormatException {
        String query = "select count(*) from " + tableName;
        Statement statement = clickHouseConnection.getStatement();
        statement.closeOnCompletion();
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next();
        return Integer.parseInt(resultSet.getString(1));
    }

    public List<String[]> getBatchData(String tableName, int copyCount, int batchSize, int size) throws SQLException {
        String query = "select * from " + tableName + " limit " + copyCount + ", " + copyCount + batchSize;
        Statement statement = clickHouseConnection.getStatement();
        statement.closeOnCompletion();
        ResultSet resultSet = statement.executeQuery(query);
        List<String[]> data = new ArrayList<>();
        while (resultSet.next()) {
            String[] row = new String[size];
            for (int i = 0; i < size; i++) {
                row[i] = resultSet.getString(i + 1);
            }
            data.add(row);
        }
        return data;
    }

    public void closeConnection() {
        clickHouseConnection.close();
    }

    public void importCsv(File file, TextArea logArea) throws IOException, SQLException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = "";
        String tableName = null;
        int connReset = 1;
        boolean firstLine = true;
        Map<String, Type> map = new LinkedHashMap<>();
        while ((line = bufferedReader.readLine()) != null) {
            String[] separated = line.split(";");
            if (firstLine) {
                TextInputDialog dialog = new TextInputDialog();
                dialog.setTitle("Podaj nazwe dla tabeli");
                dialog.setHeaderText("Tworzenie nowej tabeli");
                dialog.setContentText("Podaj nazwe dla tabeli");
                Optional<String> result = dialog.showAndWait();
                if (result.isPresent()) {
                    tableName = result.get();
                } else {
                    throw new IllegalArgumentException("Nie podano nazwy");
                }
                String[] columnsName = separated;
                columnsName = prepColsName(columnsName);
                for (int i = 0; i < columnsName.length; i++) {
                    map.put(columnsName[i], getType(columnsName[i]));
                }

                String query = prepareCreateTableStatement(tableName, map);

                System.out.println(query);

                Statement statement = clickHouseConnection.getStatement();
                statement.closeOnCompletion();
                statement.execute(query);

                firstLine = false;
                logArea.appendText(LocalDate.now().toString() + " - Utworzono tabelę " + tableName + "\n");
                logArea.appendText(LocalDate.now().toString() + " - rozpoczęto importowanie\n");
                continue;
            }
            String query = "insert into " + tableName + " values (";
            for (int i = 0; i < separated.length; i++) {
                if (separated[i].equals("") || separated[i] == null) {
                    query += "null";
                } else if (isNumeric(separated[i])) {
                    query += separated[i];
                } else {
                    if (separated[i].contains("'")) {
                        separated[i] = separated[i].replaceAll("'", "''");
                    }
                    query += "'" + separated[i] + "'";
                }
                if (i != separated.length - 1) {
                    query += ", ";
                } else {
                    query += ")";
                }
            }
            Statement statement = clickHouseConnection.getStatement();
            statement.closeOnCompletion();
            statement.execute(query);
            clickHouseConnection.close();
        }
        logArea.appendText(LocalDate.now().toString() + " - zakończono importowanie\n");
    }

    private Type getType(String columnName) {
        List types = new ArrayList<>(Arrays.asList("SMALLINT",
                "Boolean",
                "Date",
                "DateTime",
                "Enum",
                "FixedString",
                "Float32",
                "Float64",
                "UInt8",
                "UInt16",
                "UInt32",
                "UInt64",
                "Int8",
                "Int16",
                "Int32",
                "Int64",
                "String"));
        ChoiceDialog<String> choiceDialog = new ChoiceDialog<>("VARCHAR", types);
        choiceDialog.setContentText("Wybierz typ danych dla kolumny " + columnName);
        choiceDialog.setHeaderText("Wybierz typ");
        choiceDialog.setTitle("Wybierz typ");
        Optional<String> typeResult = choiceDialog.showAndWait();
        if (typeResult.isPresent()) {
            if (typeResult.get().equals("Fixedtring")) {
                TextInputDialog sizeDialog = new TextInputDialog();
                sizeDialog.setTitle("Podaj rozmiar");
                sizeDialog.setContentText("Podaj rozmiar");
                Optional<String> sizeResult = sizeDialog.showAndWait();
                if (sizeResult.isPresent()) {
                    return new Type(typeResult.get(), Integer.valueOf(sizeResult.get()));
                } else {
                    throw new IllegalArgumentException("Nie podano rozmiaru dla typu");
                }
            }
            return new Type(typeResult.get());
        } else {
            throw new IllegalArgumentException("Nie wybrano typu");
        }
    }

    private String[] prepColsName(String[] columnsName) {
        for (int i = 0; i < columnsName.length; i++) {
            columnsName[i] = columnsName[i].replaceAll(" ", "");
        }
        return columnsName;
    }
}
