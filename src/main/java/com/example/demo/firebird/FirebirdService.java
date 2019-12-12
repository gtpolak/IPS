package com.example.demo.firebird;

import com.example.demo.models.Type;
import com.example.demo.scenesControllers.MainSceneController;
import javafx.scene.control.ChoiceDialog;
import javafx.scene.control.TextInputDialog;
import org.springframework.stereotype.Component;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Component
public class FirebirdService {

    private final FirebirdConnector fireBirdConnector;

    public FirebirdService(FirebirdConnector fireBirdConnector) {
        this.fireBirdConnector = fireBirdConnector;
    }

    public Set<String> getAllTables() throws SQLException {
        Set<String> tabsNames = new HashSet<>();
        Statement statement = fireBirdConnector.createStatement();
        try {
            ResultSet resultSet = statement.executeQuery("select rdb$relation_name\n" +
                    "from rdb$relations\n" +
                    "where rdb$view_blr is null\n" +
                    "and (rdb$system_flag is null or rdb$system_flag = 0)");
            while (resultSet.next()) {
                tabsNames.add(resultSet.getString(1).replaceAll(" ", ""));
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tabsNames;
    }

    public Map<String, Type> getColumnNameAndTypeFromTable(String tableName) throws SQLException {
        tableName = tableName.toUpperCase();
        Map<String, Type> columnsNameAntTypes = new LinkedHashMap<>();
        //fireBirdConnector.beginTransaction();
        String query = "SELECT\n" +
                "  R.RDB$FIELD_NAME,\n" +
                "  F.RDB$FIELD_TYPE,\n" +
                "  F.RDB$FIELD_LENGTH\n" +
                "FROM\n" +
                "  RDB$RELATION_FIELDS R\n" +
                "  JOIN RDB$FIELDS F\n" +
                "    ON F.RDB$FIELD_NAME = R.RDB$FIELD_SOURCE\n" +
                "  JOIN RDB$RELATIONS RL\n" +
                "    ON RL.RDB$RELATION_NAME = R.RDB$RELATION_NAME\n" +
                "WHERE\n" +
                "  COALESCE(R.RDB$SYSTEM_FLAG, 0) = 0\n" +
                "  AND\n" +
                "  COALESCE(RL.RDB$SYSTEM_FLAG, 0) = 0\n" +
                "  AND\n" +
                "  RL.RDB$VIEW_BLR IS NULL\n" +
                "  AND\n" +
                "  R.RDB$RELATION_NAME = '" + tableName + "'\n" +
                "ORDER BY\n" +
                "  R.RDB$RELATION_NAME,\n" +
                "  R.RDB$FIELD_POSITION";

        Statement statement = fireBirdConnector.createStatement();
        statement.closeOnCompletion();
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            columnsNameAntTypes.put(resultSet.getString(1).replaceAll(" ", ""), new Type(convertToType(resultSet.getString(2)), Integer.valueOf(resultSet.getString(3))));
        }

        return columnsNameAntTypes;
    }

    private String convertToType(String type) {
        switch (type) {
            case "7":
                return "SMALLINT";
            case "8":
                return "INTEGER";
            case "10":
                return "FLOAT";
            case "12":
                return "DATE";
            case "13":
                return "TIME";
            case "14":
                return "CHAR";
            case "16":
                return "BIGINT";
            case "27":
                return "DOUBLE PRECISION";
            case "35":
                return "TIMESTAMP";
            case "37":
                return "VARCHAR";
            case "261":
                return "BLOB";
            default:
                throw new IllegalArgumentException("Unknown DB column type");
        }
    }

    public boolean createTable(String tableName, Map<String, Type> columnNameAndType) {
        try {
            String sql = "CREATE TABLE " + tableName + "(\n";
            Iterator<Map.Entry<String, Type>> iterator = columnNameAndType.entrySet().iterator();
            int i = 1;
            while (iterator.hasNext()) {

                Map.Entry<String, Type> nameAndType = iterator.next();
                //TODO: check for different types
                if (nameAndType.getValue().getTypeName().contains("VARCHAR") || nameAndType.getValue().getTypeName().contains("CHAR")) {
                    if (i != columnNameAndType.size()) {
                        sql += nameAndType.getKey() + " " + nameAndType.getValue().getTypeName() + "(" + nameAndType.getValue().getTypeSize() + "),\n";
                    } else {
                        sql += nameAndType.getKey() + " " + nameAndType.getValue().getTypeName() + "(" + nameAndType.getValue().getTypeSize() + ")\n";
                    }
                } else {
                    if (i != columnNameAndType.size()) {
                        sql += nameAndType.getKey() + " " + nameAndType.getValue().getTypeName() + ",\n";
                    } else {
                        sql += nameAndType.getKey() + " " + nameAndType.getValue().getTypeName() + "\n";
                    }
                }

                i++;
            }
            sql += ");";
            Statement statement = fireBirdConnector.createStatement();
            statement.closeOnCompletion();
            statement.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public int getRowsCount(String tableName) throws SQLException {
        Statement statement = fireBirdConnector.createStatement();
        try {
            statement.closeOnCompletion();
            ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName);
            resultSet.next();
            return Integer.parseInt(resultSet.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public List<String[]> getDataForBatchInsert(String tableName, int sizeOfBatch, int copiedRows, int numberOfCols) throws SQLException {
        Statement statement = fireBirdConnector.createStatement();
        statement.closeOnCompletion();
        String query = "select * from " + tableName + " rows " + (copiedRows) + " to " + (copiedRows + sizeOfBatch - 1);
        ResultSet resultSet = statement.executeQuery(query);
        List<String[]> data = new ArrayList<>();
        while (resultSet.next()) {
            String[] row = new String[numberOfCols];
            for (int i = 0; i < numberOfCols; i++) {
                row[i] = resultSet.getString(i + 1);
            }
            data.add(row);
        }
        return data;
    }

    public void importCsv(File file, MainSceneController mainSceneController) throws IOException, NullPointerException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        final String[] line = {""};

        String tableName;
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

        String finalTableName = tableName;
        try {
            if (getAllTables().stream().anyMatch(s -> s.equals(finalTableName))) {
                mainSceneController.clearAndAppendToLogArea("Tabela " + tableName + " już istnieje w bazie Firebird");
                return;
            }
        } catch (SQLException e) {
            mainSceneController.clearAndAppendToLogArea("Błąd podczas pobierania danych z bazy Firebird");
            return;
        }

        String separator = getSeparator();

        line[0] = bufferedReader.readLine();
        if (line[0] == null) {
            throw new IllegalArgumentException("Plik jest pusty");
        }
        String[] columnsName = line[0].split(separator);
        columnsName = prepColsName(columnsName);
        Map<String, Type> colTypes = new LinkedHashMap<>();
        for (int i = 0; i < columnsName.length; i++) {
            colTypes.put(columnsName[i], getType(columnsName[i]));
        }

        String createTableQuery = prepareCreateTableStatement(tableName, colTypes);

        try {
            Statement statement = fireBirdConnector.createStatement();
            statement.closeOnCompletion();
            statement.execute(createTableQuery);
        } catch (SQLException e){
            mainSceneController.clearAndAppendToLogArea("Błąd podczas tworzenia tabeli w bazie. Wygenerowany SQL:\n" + createTableQuery);
        }
        mainSceneController.appendToLogArea("Utworzono tabelę " + tableName + "\n");
        mainSceneController.refreshFirebirdTables();
        mainSceneController.appendToLogArea("Rozpoczęto importowanie pliku " + file.getName() + " do bazy Firebird\n");


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
        new Thread(() -> {
            int execBatch = 1;
            PreparedStatement pstmt = null;
            try {
                pstmt = fireBirdConnector.getConnection().prepareStatement(insert.toString());
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }
            while (true) {
                try {
                    if ((line[0] = bufferedReader.readLine()) == null) break;
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                String[] separated = line[0].split(separator);
                AtomicInteger index = new AtomicInteger(0);
                try {
                    for (Map.Entry<String, Type> entry : colTypes.entrySet()) {
                        switch (entry.getValue().getTypeName()) {
                            case "VARCHAR":
                                if (separated[index.get()] == null || separated[index.get()].equals("")) {
                                    separated[index.get()] = "null";
                                }
                                pstmt.setString(index.get() + 1, separated[index.get()]);
                                break;
                            case "CHAR":
                                if (separated[index.get()] == null || separated[index.get()].equals("")) {
                                    separated[index.get()] = "0";
                                }
                                pstmt.setNString(index.get() + 1, separated[index.get()]);
                            case "INTEGER":
                            case "SMALLINT":
                                if (separated[index.get()] == null || separated[index.get()].equals("")) {
                                    separated[index.get()] = "0";
                                }
                                pstmt.setInt(index.get() + 1, Integer.parseInt(separated[index.get()]));
                                break;
                            case "BIGINT":
                                if (separated[index.get()] == null || separated[index.get()].equals("")) {
                                    separated[index.get()] = "0";
                                }
                                pstmt.setLong(index.get() + 1, Long.parseLong(separated[index.get()]));
                                break;
                            case "DATE":
                                pstmt.setDate(index.get() + 1, Date.valueOf(separated[index.get()]));
                                break;
                            case "TIME":
                                pstmt.setTime(index.get() + 1, Time.valueOf(separated[index.get()]));
                                break;
                            case "FLOAT":
                                pstmt.setFloat(index.get() + 1, Float.parseFloat(separated[index.get()]));
                                break;
                            case "DOUBLE PRECISION":
                                if (separated[index.get()] == null || separated[index.get()].equals("")) {
                                    separated[index.get()] = "0";
                                }
                                pstmt.setDouble(index.get() + 1, Double.parseDouble(separated[index.get()]));
                                break;
                            case "TIMESTAMP":
                                pstmt.setTimestamp(index.get() + 1, Timestamp.valueOf(separated[index.get()]));
                                break;
                        }
                        index.incrementAndGet();
                    }
                    pstmt.addBatch();
                    if (execBatch % 1000 != 0) {
                        execBatch++;
                        continue;
                    }

                    pstmt.executeBatch();

                    execBatch = 1;
                } catch (SQLException e) {
                    e.printStackTrace();
                    mainSceneController.clearAndAppendToLogArea("Błąd podczas wstawiania danych\n");
                    break;
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    return;
                }
            }
            try {
                pstmt.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
                mainSceneController.clearAndAppendToLogArea("Błąd podczas wstawiania danych z pliku " + file.getName() + "\n");
                return;
            }
            try {
                pstmt.close();
            } catch (SQLException e) {
                mainSceneController.clearAndAppendToLogArea("Błąd podczas zamykania połączenia\n");
            }
            mainSceneController.appendToLogArea("Zakończono importowanie pliku " + file.getName() + " do bazy Firebird\n");

            try {
                bufferedReader.close();
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            mainSceneController.refreshFirebirdTables();
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

    private String prepareCreateTableStatement(String tableName, Map<String, Type> map) {
        String query = "create table " + tableName + "(\n";

        int i = 1;
        for (Map.Entry<String, Type> entry : map.entrySet()) {
            if (i != map.size()) {
                query += entry.getKey() + " " + entry.getValue().getTypeName();
                if (entry.getValue().getTypeSize() != null) {
                    query += "(" + entry.getValue().getTypeSize() + ")";
                }
                query += ",\n";
            } else {
                query += entry.getKey() + " " + entry.getValue().getTypeName();
                if (entry.getValue().getTypeSize() != null) {
                    query += "(" + entry.getValue().getTypeSize() + ")\n";
                }
                query += "\n";
            }
            i++;
        }
        query += ");";
        return query;
    }

    private Type getType(String columnName) {
        List types = new ArrayList<>(Arrays.asList("SMALLINT",
                "INTEGER",
                "FLOAT",
                "DATE",
                "BIGINT",
                "DOUBLE PRECISION",
                "TIMESTAMP",
                "VARCHAR"));
        ChoiceDialog<String> choiceDialog = new ChoiceDialog<>("VARCHAR", types);
        choiceDialog.setContentText("Wybierz typ danych dla kolumny " + columnName);
        choiceDialog.setHeaderText("Wybierz typ");
        choiceDialog.setTitle("Wybierz typ");
        Optional<String> typeResult = choiceDialog.showAndWait();
        if (typeResult.isPresent()) {
            if (typeResult.get().equals("VARCHAR") || typeResult.get().equals("CHAR")) {
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

    public void dropTable(String tableName) throws SQLException {
        String query = "drop table " + tableName;
        Statement statement = fireBirdConnector.createStatement();
        statement.closeOnCompletion();
        statement.execute(query);
    }

    public boolean isNumeric(String strNum) {
        Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");

        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }

    public Map<String, Type> convertToFirebirdDataTypes(Map<String, Type> tableDesc) {
        for (Map.Entry<String, Type> entry : tableDesc.entrySet()) {
            entry.setValue(getType(entry.getKey()));
        }
        return tableDesc;
    }

    public void insertBatchData(List<String[]> data, Map<String, Type> colTypes, String tableName) throws SQLException {
//        List<String> inserts = new ArrayList<>();
//        data.forEach(strings -> {
//            inserts.add(createInsertStatement(strings, colTypes, tableName));
//        });

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

        final PreparedStatement[] preparedStatement = {fireBirdConnector.getConnection().prepareStatement(insert.toString())};
        data.forEach(colData -> {
            try {
                preparedStatement[0] = prepareInsertStmt(preparedStatement[0], colData, colTypes);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        preparedStatement[0].executeBatch();
        preparedStatement[0].close();

//        Statement statement = fireBirdConnector.createStatement();
//        for (String insert : inserts) {
//            statement.execute(insert);
//        }
//        statement.close();
    }

    private PreparedStatement prepareInsertStmt(PreparedStatement pstmt, String[] colData, Map<String, Type> colTypes) {
        AtomicInteger index = new AtomicInteger(0);
        for (Map.Entry<String, Type> entry : colTypes.entrySet()) {
            try {
                switch (entry.getValue().getTypeName()) {
                    case "VARCHAR":
                    case "CHAR":
                        pstmt.setString(index.get() + 1, colData[index.get()]);
                        break;
                    case "INTEGER":
                    case "SMALLINT":
                        pstmt.setInt(index.get() + 1, Integer.parseInt(colData[index.get()]));
                        break;
                    case "BIGINT":
                        pstmt.setLong(index.get() + 1, Long.parseLong(colData[index.get()]));
                        break;
                    case "DATE":
                        pstmt.setDate(index.get() + 1, Date.valueOf(colData[index.get()]));
                        break;
                    case "TIME":
                        pstmt.setTime(index.get() + 1, Time.valueOf(colData[index.get()]));
                        break;
                    case "FLOAT":
                        pstmt.setFloat(index.get() + 1, Float.parseFloat(colData[index.get()]));
                        break;
                    case "DOUBLE PRECISION":
                        pstmt.setDouble(index.get() + 1, Double.parseDouble(colData[index.get()]));
                        break;
                    case "TIMESTAMP":
                        pstmt.setTimestamp(index.get() + 1, Timestamp.valueOf(colData[index.get()]));
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

    private String createInsertStatement(String[] strings, Map<String, Type> firebirdType, String tableName) {
        AtomicInteger index = new AtomicInteger(0);
        final String[] query = new String[]{"insert into " + tableName + " values ("};
        for (Map.Entry<String, Type> entry : firebirdType.entrySet()) {
            if (entry.getValue().getTypeName().contains("INT") || entry.getValue().getTypeName().contains("FLOAT")
                    || entry.getValue().getTypeName().contains("DOUBLE")) {
                if (index.get() == firebirdType.size() - 1) {
                    query[0] += strings[index.getAndIncrement()] + ")";
                } else {
                    query[0] += strings[index.getAndIncrement()] + ", ";
                }
            } else {
                if (strings[index.get()].contains("'")) {
                    strings[index.get()] = strings[index.get()].replaceAll("'", "''");
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

    public void closeConnection() throws SQLException {
        fireBirdConnector.close();
    }
}
