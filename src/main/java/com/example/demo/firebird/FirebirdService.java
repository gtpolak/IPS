package com.example.demo.firebird;

import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

        return tabsNames;
    }
}
