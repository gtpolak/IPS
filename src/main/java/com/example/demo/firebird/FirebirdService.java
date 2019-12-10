package com.example.demo.firebird;

import com.example.demo.models.Type;
import javafx.scene.control.ChoiceDialog;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextInputDialog;
import org.springframework.stereotype.Component;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Component
public class FirebirdService {

    private final FirebirdConnector fireBirdConnector;

    public FirebirdService(FirebirdConnector fireBirdConnector) {
        this.fireBirdConnector = fireBirdConnector;
    }

    public Set<String> getAllTables() {
        Set<String> tabsNames = new HashSet<>();
        Statement statement = fireBirdConnector.createStatement();
        try {
            statement.closeOnCompletion();
            ResultSet resultSet = statement.executeQuery("select rdb$relation_name\n" +
                    "from rdb$relations\n" +
                    "where rdb$view_blr is null\n" +
                    "and (rdb$system_flag is null or rdb$system_flag = 0)");
            while (resultSet.next()) {
                tabsNames.add(resultSet.getString(1).replaceAll(" ", ""));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

//        List<String> resultList = fireBirdConnector.getSession().createNativeQuery("select rdb$relation_name\n" +
//                "from rdb$relations\n" +
//                "where rdb$view_blr is null\n" +
//                "and (rdb$system_flag is null or rdb$system_flag = 0)").list();
//        //tabsNames = resultList.stream().map(s -> s.replaceAll(" ", "")).collect(Collectors.toSet());
//        //fireBirdConnector.commitTransaction();
        return tabsNames;
    }

    public Map<String, Type> getColumnNameAndTypeFromTable(String tableName) {
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
        try {
            Statement statement = fireBirdConnector.createStatement();
            statement.closeOnCompletion();
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                columnsNameAntTypes.put(resultSet.getString(1).replaceAll(" ", ""), new Type(convertToType(resultSet.getString(2)), Integer.valueOf(resultSet.getString(3))));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

//        List<Object[]> result = fireBirdConnector.getSession().createNativeQuery().list();
//        result.forEach(objects -> {
//            columnsNameAntTypes.put(objects[0].toString().replaceAll(" ", ""), new Type(convertToType(objects[1].toString()), objects[2].toString()));
//        });
//        fireBirdConnector.commitTransaction();
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
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public int getRowsCount(String tableName) {
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
        while(resultSet.next()){
            String[] row = new String[numberOfCols];
            for(int i = 0; i < numberOfCols; i++){
                row[i] = resultSet.getString(i+1);
            }
            data.add(row);
        }
        return data;
    }

    public void importCsv(File file, TextArea logArea) throws FileNotFoundException, IOException, SQLException, NullPointerException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = "";

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
        line = bufferedReader.readLine();
        if(line == null){
            throw new IllegalArgumentException("Plik jest pusty");
        }
        String[] columnsName = line.split(";");
        columnsName = prepColsName(columnsName);
        Map<String, Type> colTypes = new LinkedHashMap<>();
        for (int i = 0; i < columnsName.length; i++) {
            colTypes.put(columnsName[i], getType(columnsName[i]));
        }

        String createTableQuery = prepareCreateTableStatement(tableName, colTypes);

        System.out.println(createTableQuery);

        Statement statement = fireBirdConnector.createStatement();
        statement.closeOnCompletion();
        statement.execute(createTableQuery);
        logArea.appendText(LocalDateTime.now().toString() + " - Utworzono tabelę " + tableName +"\n");
        logArea.appendText(LocalDateTime.now().toString() + " - rozpoczęto importowanie\n");
        int resetConn = 1;
        String queries = "";
        while ((line = bufferedReader.readLine()) != null) {
            String[] separated = line.split(";");

            String query = "insert into " + tableName + " values (";
            for(int i = 0; i < separated.length; i++){
                if(separated[i].equals("") || separated[i] == null){
                    query += "null";
                } else if(isNumeric(separated[i])){
                    query += separated[i];
                } else {
                    if(separated[i].contains("'")){
                        separated[i] = separated[i].replaceAll("'", "''");
                    }
                    query += "'" + separated[i] + "'";
                }
                if(i != separated.length-1){
                    query += ", ";
                } else {
                    query += ")";
                }
            }
            queries += query + "; ";
            if(resetConn%30 != 0){
                resetConn++;
                continue;
            }
            statement = fireBirdConnector.createStatement();
            statement.closeOnCompletion();
            try {
                statement.execute(query);
                queries = "";
                resetConn = 1;
            }catch (SQLException e){
                e.printStackTrace();
                System.out.println(query);
                break;
            }
        }
        logArea.appendText(LocalDateTime.now().toString() + " - zakończono importowanie\n");
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
                "TIME",
                "CHAR",
                "BIGINT",
                "DOUBLE PRECISION",
                "TIMESTAMP",
                "VARCHAR",
                "BLOB"));
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
        for(Map.Entry<String, Type> entry : tableDesc.entrySet()){
            entry.setValue(getType(entry.getKey()));
        }
        return tableDesc;
    }

    public void insertBatchData(List<String[]> data, Map<String, Type> firebirdType, String tableName) throws SQLException {
        List<String> inserts = new ArrayList<>();
        data.forEach(strings -> {
            inserts.add(createInsertStatement(strings, firebirdType, tableName));
        });

        Statement statement = fireBirdConnector.createStatement();
        for (String insert : inserts) {
            statement.execute(insert);
        }
        statement.close();
    }

    private String createInsertStatement(String[] strings, Map<String, Type> firebirdType, String tableName) {
        AtomicInteger index = new AtomicInteger(0);
        final String[] query = new String[]{"insert into " + tableName + " values ("};
        for(Map.Entry<String, Type> entry : firebirdType.entrySet()){
            if(entry.getValue().getTypeName().contains("INT") || entry.getValue().getTypeName().contains("FLOAT")
                    || entry.getValue().getTypeName().contains("DOUBLE")){
                if(index.get() == firebirdType.size()-1){
                    query[0] += strings[index.getAndIncrement()] + ")";
                } else {
                    query[0] += strings[index.getAndIncrement()] + ", ";
                }
            } else {
                if(strings[index.get()].contains("'")){
                    strings[index.get()] = strings[index.get()].replaceAll("'", "''");
                }
                if(index.get() == firebirdType.size()-1){
                    query[0] += "'" + strings[index.getAndIncrement()] + "')";
                } else {
                    query[0] += "'" + strings[index.getAndIncrement()] + "', ";
                }
            }
        }
        return query[0];
    }

    public void closeConnection() {
        fireBirdConnector.close();
    }
}
