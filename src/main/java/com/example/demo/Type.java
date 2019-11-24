package com.example.demo;

public class Type {

    private String typeName;
    private String typeSize;

    public Type(String typeName, String typeSize) {
        this.typeName = typeName;
        this.typeSize = typeSize;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeSize() {
        return typeSize;
    }

    public void setTypeSize(String typeSize) {
        this.typeSize = typeSize;
    }
}
