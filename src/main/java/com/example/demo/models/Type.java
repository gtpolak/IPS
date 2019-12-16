package com.example.demo.models;

public class Type {

    private String typeName;
    private Integer typeSize;

    public Type(String typeName, Integer typeSize) {
        this.typeName = typeName;
        this.typeSize = typeSize;
    }

    public Type(String typeName) {
        this.typeName = typeName;
        this.typeSize = null;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Integer getTypeSize() {
        return typeSize;
    }

    public void setTypeSize(Integer typeSize) {
        this.typeSize = typeSize;
    }
}
