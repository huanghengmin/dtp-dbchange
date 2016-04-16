package com.inetec.ichange.plugin.dbchange.datautils.db;

public class Operator {

    public final static String Str_Operator = "operator";
    public final static String Str_OPERATOR__INSERT = "insert";
    public final static String Str_OPERATOR__UPDATE = "update";
    public final static String Str_OPERATOR__DELETE = "delete";

    public final static Operator OPERATOR__INSERT = new Operator("insert");
    public final static Operator OPERATOR__UPDATE = new Operator("update");
    public final static Operator OPERATOR__DELETE = new Operator("delete");


    private String action;

    private Operator(String name) {
        action = name;
    }

    public static Operator getOperator(String name) {
        Operator result = null;
        if (name.equals(Str_OPERATOR__DELETE)) {
            result = OPERATOR__DELETE;
        }
        if (name.equals(Str_OPERATOR__INSERT))
            result = OPERATOR__INSERT;
        if (name.equals(Str_OPERATOR__UPDATE))
            result = OPERATOR__UPDATE;
        return result;
    }

    public boolean isInsertAction() {
        return this == OPERATOR__INSERT;
    }

    public boolean isUpdateAction() {
        return this == OPERATOR__UPDATE;
    }

    public boolean isDeleteAction() {
        return this == OPERATOR__DELETE;
    }

    public String toString() {
        return action;
    }

}
