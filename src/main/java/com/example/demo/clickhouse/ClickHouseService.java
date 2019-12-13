package com.example.demo.clickhouse;

import com.example.demo.models.Type;
import com.example.demo.scenesControllers.MainSceneController;
import javafx.scene.control.ChoiceDialog;
import javafx.scene.control.TextInputDialog;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Component
public class ClickHouseService {

    private final ClickHouseConnection clickHouseConnection;

    public ClickHouseService(ClickHouseConnection clickHouseConnection) {
        this.clickHouseConnection = clickHouseConnection;
    }

    public Set<String> getAllTables() throws SQLException {
        Set<String> dbsList = new LinkedHashSet<>();
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
        columnNameAndType = convertToClickHouseTypes(columnNameAndType);

        String sql = prepareCreateTableStatement(tableName, columnNameAndType);

        Connection connection = clickHouseConnection.getConnection();
        try {
            Statement statement = connection.createStatement();
            statement.executeQuery(sql);

            statement.close();
        } catch (SQLException e){
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
                case "VARCHAR":
                    entry.getValue().setTypeName("String");
                    break;
                case "DOUBLE PRECISION":
                    entry.getValue().setTypeName("Float64");
                    break;
                case "TIMESTAMP":
                    entry.getValue().setTypeName("DateTime");
                    break;
                case "CHAR":
                    if (entry.getValue().getTypeSize() == 1) {
                        entry.getValue().setTypeName("Boolean");
                        break;
                    }
                    entry.getValue().setTypeName("String");
                    break;
                default:
                    throw new IllegalArgumentException("Błąd podczas tłumaczenia typu danych z firebird do Clickhouse");
            }
        }
        return columnNameAndType;
    }

    public void insertBatchData(String tableName, Map<String, Type> colTypes, List<String[]> data) throws SQLException {
        //String query = createInsertStatement(tableName, colTypes, data);

        StringBuilder insert = new StringBuilder("insert into " + tableName + " values (");
        int colIndex = 1;
        for (Map.Entry<String, Type> entry : colTypes.entrySet()) {
            if (colIndex == colTypes.size()) {
                insert.append("?)");
                break;
            }
            insert.append("?, ");
            colIndex++;
        }

        final PreparedStatement[] preparedStatement = {clickHouseConnection.getConnection().prepareStatement(insert.toString())};
        data.forEach(colData -> {
            try {
                preparedStatement[0] = prepareInsertStmt(preparedStatement[0], colData, colTypes);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        preparedStatement[0].executeBatch();
        preparedStatement[0].close();
    }

    private PreparedStatement prepareInsertStmt(PreparedStatement pstmt, String[] colData, Map<String, Type> colTypes) {
        AtomicInteger index = new AtomicInteger(0);
        for (Map.Entry<String, Type> entry : colTypes.entrySet()) {
            try {
                switch (entry.getValue().getTypeName()) {
                    case "String":
                        if (colData[index.get()] == null || colData[index.get()].equals("")) {
                            colData[index.get()] = "null";
                        }
                        pstmt.setString(index.get() + 1, String.valueOf(colData[index.get()]));
                        break;
                    case "Int32":
                    case "Int8":
                    case "Int16":
                    case "Int64":
                    case "UInt32":
                    case "UInt8":
                    case "UInt16":
                    case "UInt64":
                        if (colData[index.get()] == null || colData[index.get()].equals("")) {
                            pstmt.setFloat(index.get() + 1, 0);
                            break;
                        }
                        pstmt.setInt(index.get() + 1, Integer.parseInt(colData[index.get()]));
                        break;
                    case "Float32":
                    case "Float64":
                        if (colData[index.get()] == null || colData[index.get()].equals("")) {
                            pstmt.setFloat(index.get() + 1, 0);
                            break;
                        }
                        pstmt.setFloat(index.get() + 1, Float.parseFloat(colData[index.get()]));
                        break;
                    case "DateTime":
                        if (colData[index.get()] == null || colData[index.get()].equals("")) {
                            pstmt.setTimestamp(index.get() + 1, Timestamp.valueOf(LocalDateTime.now()));
                            break;
                        }
                        pstmt.setTimestamp(index.get() + 1, Timestamp.valueOf(colData[index.get()]));
                        break;
                    case "Boolean":
                        if (colData[index.get()] == null || colData[index.get()].equals("")) {
                            pstmt.setBoolean(index.get() + 1, false);
                            break;
                        }
                        if (colData[index.get()].equals("0")) {
                            pstmt.setBoolean(index.get() + 1, false);
                        } else {
                            pstmt.setBoolean(index.get() + 1, true);
                        }
                        break;
                    case "Date":
                        if (colData[index.get()] == null || colData[index.get()].equals("")) {
                            pstmt.setDate(index.get() + 1, Date.valueOf(LocalDate.now()));
                            break;
                        }
                        pstmt.setDate(index.get() + 1, Date.valueOf(colData[index.get()]));
                        break;
                }
                index.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            pstmt.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }

    public String createInsertStatement(String tableName, Map<String, Type> colsTypes, List<String[]> data) {
        final String[] query = {"insert into " + tableName + " values "};
        data.forEach(strings -> {
            query[0] += createInsertStatement(strings, colsTypes) + " ";
        });
        return query[0];
    }

    public boolean isNumeric(String strNum) {
        Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");

        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }

    private String createInsertStatement(String[] strings, Map<String, Type> firebirdType) {
        try {
            AtomicInteger index = new AtomicInteger(0);
            final String[] query = new String[]{"("};
            for (Map.Entry<String, Type> entry : firebirdType.entrySet()) {
                if (entry.getValue().getTypeName().contains("Int") || entry.getValue().getTypeName().contains("Float")) {
                    if (strings[index.get()] == null || strings[index.get()].equals("")) {
                        strings[index.get()] = "0";
                    }
                    if (index.get() == firebirdType.size() - 1) {
                        query[0] += strings[index.getAndIncrement()] + ")";
                    } else {
                        query[0] += strings[index.getAndIncrement()] + ", ";
                    }
                } else {
                    if (strings[index.get()] == null || strings[index.get()].equals("")) {
                        strings[index.get()] = "null";
                    }
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
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
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
        String query = "select * from " + tableName + " limit " + copyCount + ", " + batchSize;
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

    public void closeConnection() throws SQLException {
        clickHouseConnection.close();
    }

    public void importCsv(File file, MainSceneController mainSceneController) throws IOException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String lineSeparator = getSeparator();

        String tableName = null;
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

        try {
            String finalTableName1 = tableName;
            if(getAllTables().stream().anyMatch(s -> s.equals(finalTableName1))){
                mainSceneController.clearAndAppendToLogArea("Tabela " + tableName + " już istnieje w bazie ClickHouse");
                return;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            mainSceneController.clearAndAppendToLogArea("Błąd podczas pobierania nazw tabel z bazy ClickHouse");
            return;
        }


        final String[] line = {bufferedReader.readLine()};
        if (line[0] == null) {
            throw new IllegalArgumentException("Plik jest pusty");
        }
        String[] columnsName = line[0].split(lineSeparator);
        Map<String, Type> colTypes = new LinkedHashMap<>();
        columnsName = prepColsName(columnsName);
        for (int i = 0; i < columnsName.length; i++) {
        }
        for (int i = 0; i < columnsName.length; i++) {
            colTypes.put(columnsName[i], getType(columnsName[i]));
        }
        String query = prepareCreateTableStatement(tableName, colTypes);
        final Statement[] statement;
        try {
            statement = new Statement[]{clickHouseConnection.getStatement()};
            statement[0].closeOnCompletion();
            statement[0].execute(query);
        } catch (SQLException e) {
            mainSceneController.clearAndAppendToLogArea("Błąd podczas tworzenia tabeli. Wygenerowany SQL: \n" + query);
            return;
        }
        mainSceneController.appendToLogArea("Utworzono tabelę " + tableName + "\n");
        mainSceneController.appendToLogArea("Rozpoczęto importowanie pliku " + file.getName() + " do bazy ClickHoue\n");


        final String[] insertQuery = {"insert into " + tableName + " values "};
        String finalTableName = tableName;
        new Thread(() -> {
            int a = 1;
            while (true) {
                try {
                    if ((line[0] = bufferedReader.readLine()) == null) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String[] separated = line[0].split(lineSeparator);
                if (a % 1000 != 0) {
                    insertQuery[0] += prepareInsert(colTypes, separated) + " ";
                    a++;
                    continue;
                }
                try {
                    statement[0] = clickHouseConnection.getStatement();
                    statement[0].closeOnCompletion();
                    statement[0].execute(insertQuery[0]);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                insertQuery[0] = "insert into " + finalTableName + " values ";
                a = 1;
            }
            try {
                bufferedReader.close();
                fileReader.close();
                statement[0] = clickHouseConnection.getStatement();
                statement[0].closeOnCompletion();
                statement[0].execute(insertQuery[0]);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            mainSceneController.appendToLogArea("Zakończono importowanie pliku " + file.getName() + " do bazy ClickHouse\n");

            try {
                bufferedReader.close();
                fileReader.close();
            } catch (IOException e) {
                mainSceneController.appendToLogArea("Błąd podczas zamykania pliku");
                e.printStackTrace();
            }
        }).start();
    }

    private String getSeparator() {
        TextInputDialog dialog = new TextInputDialog();
        dialog.setTitle("Podaj separator");
        dialog.setHeaderText("Wpsz separator dla kolumn z csv");
        dialog.setContentText("Wpisz znak jakim są dzielone kolumny w pliku");
        Optional<String> result = dialog.showAndWait();
        if (result.isPresent()) {
            return result.get();
        } else {
            throw new IllegalArgumentException("Nie podano separatora");
        }
    }

    private String prepareInsert(Map<String, Type> map, String[] separated) {
        String query = "(";
        AtomicInteger index = new AtomicInteger(0);
        for (Map.Entry<String, Type> entry : map.entrySet()) {
            if (entry.getValue().getTypeName().contains("Int") || entry.getValue().getTypeName().contains("Float")) {
                if (index.get() == map.size() - 1) {
                    query += separated[index.getAndIncrement()] + ")";
                } else {
                    query += separated[index.getAndIncrement()] + ", ";
                }
            } else {
                if (separated[index.get()].contains("'")) {
                    separated[index.get()] = separated[index.get()].replaceAll("'", "''");
                }
                if (index.get() == map.size() - 1) {
                    query += "'" + separated[index.getAndIncrement()] + "')";
                } else {
                    query += "'" + separated[index.getAndIncrement()] + "', ";
                }
            }
        }
        return query;
    }

    private Type getType(String columnName) {
        List types = new ArrayList<>(Arrays.asList(
                "Boolean",
                "Date",
                "DateTime",
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
        ChoiceDialog<String> choiceDialog = new ChoiceDialog<>(types.get(0), types);
        choiceDialog.setContentText("Wybierz typ danych dla kolumny " + columnName);
        choiceDialog.setHeaderText("Wybierz typ");
        choiceDialog.setTitle("Wybierz typ");
        Optional<String> typeResult = choiceDialog.showAndWait();
        if (typeResult.isPresent()) {
            if (typeResult.get().equals("FixedString")) {
                TextInputDialog sizeDialog = new TextInputDialog();
                sizeDialog.setTitle("Podaj rozmiar");
                sizeDialog.setContentText("Podaj rozmiar dla " + columnName);
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
