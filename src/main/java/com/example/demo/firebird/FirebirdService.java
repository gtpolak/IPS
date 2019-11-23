package com.example.demo.firebird;

import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class FirebirdService {

    private final FirebirdConnector fireBirdConnector;

    public FirebirdService(FirebirdConnector fireBirdConnector) {
        this.fireBirdConnector = fireBirdConnector;
    }

    public Set<String> getTablesName(){
        Set<String> tabsNames = new HashSet<>();

        fireBirdConnector.beginTransaction();
        List<String> resultList =  fireBirdConnector.getSession().createNativeQuery("select rdb$relation_name\n" +
                "from rdb$relations\n" +
                "where rdb$view_blr is null\n" +
                "and (rdb$system_flag is null or rdb$system_flag = 0)").list();
        tabsNames.addAll(resultList);
        fireBirdConnector.commitTransaction();
        return tabsNames;
    }

    public Map<String, Integer> getColumnNameAndTypeFromTable(String tableName){
        tableName = tableName.toUpperCase();
        Map<String, Integer> columnsNameAntTypes = new LinkedHashMap<>();
        fireBirdConnector.beginTransaction();
        List<Object[]> result = fireBirdConnector.getSession().createNativeQuery("SELECT\n" +
                "  R.RDB$FIELD_NAME,\n" +
                "  F.RDB$FIELD_TYPE\n" +
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
                "  R.RDB$FIELD_POSITION").list();
        result.forEach(objects -> {
            columnsNameAntTypes.put(objects[0].toString().replaceAll(" ", ""), Integer.valueOf(objects[1].toString()));
        });
        fireBirdConnector.commitTransaction();
        return columnsNameAntTypes;
    }
}
