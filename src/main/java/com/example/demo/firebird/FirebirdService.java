package com.example.demo.firebird;

import com.example.demo.Type;
import org.hibernate.Session;
import org.hibernate.hql.internal.ast.SqlASTFactory;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class FirebirdService {

    private final FirebirdConnector fireBirdConnector;

    public FirebirdService(FirebirdConnector fireBirdConnector) {
        this.fireBirdConnector = fireBirdConnector;
    }

    public Set<String> getTablesName() {
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
                columnsNameAntTypes.put(resultSet.getString(1).replaceAll(" ", ""), new Type(convertToType(resultSet.getString(2)), resultSet.getString(3)));
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
            return statement.execute(sql);
//            System.out.println(sql);
//            Session session = fireBirdConnector.getSessionFactory().getCurrentSession();
//            session.beginTransaction();
//            session.createNativeQuery(sql).executeUpdate();
//            session.getTransaction().commit();
//            session.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        //return true;
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

    public List<List<String>> getDataForBatchInsert(String tableName, int sizeOfBatch, int copiedRows, int numberOfCols) throws SQLException {
        List<List<String>> result = new ArrayList<>();
        Statement statement = fireBirdConnector.createStatement();
        statement.closeOnCompletion();
        String query = "select * from " + tableName + " rows " + (copiedRows) + " to " + (copiedRows + sizeOfBatch - 1);
        ResultSet resultSet = statement.executeQuery(query);
        while(resultSet.next()){
            List<String> row = new ArrayList<>();
            for(int i = 1; i <= numberOfCols; i++){
                row.add(resultSet.getString(i));
            }
            result.add(row);
        }
        return result;
    }
}
