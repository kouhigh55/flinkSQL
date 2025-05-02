package org.example.highjoin.entities;

import java.util.ArrayList;

public enum Relation {
    LINEITEM("LINEITEM", new String[]{}, new String[]{"ORDERS"},
            new String[]{"l_orderkey", "l_extendedprice","l_discount","l_returnflag",},
            "l_orderkey", "o_custkey"), // same as group by c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment

    ORDERS("ORDERS", new String[]{"LINEITEM"}, new String[]{"CUSTOMER"},
            new String[]{"o_orderkey", "o_custkey","o_orderdate"},
            "o_custkey", "o_orderkey"),

    CUSTOMER("CUSTOMER", new String[]{"ORDERS"}, new String[]{"NATION"},
            new String[]{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal","c_comment"},
            "c_nationkey", "c_custkey"),

    NATION("NATION", new String[]{"CUSTOMER"}, new String[]{},
            new String[]{"n_nationkey", "n_name"},
            "n_nationkey", "n_nationkey"),
    ;


    public final String name;
    public final String[] fathers;
    public final String[] children;
    public final String[] attrName;
    public final String inputKey;
    public final String outputKey;


    Relation(String name, String[] fathers, String[] children, String[] attr, String inputKey, String outputKey) {
        this.name = name;
        this.inputKey = inputKey;
        this.outputKey = outputKey;
        this.fathers = fathers;
        this.children = children;
        this.attrName = attr;
    }

    public static Relation getRelationFromName(String name){
        for (Relation relation : Relation.values()) {
            if (relation.name.toUpperCase().equals(name)) {
                return relation;
            }
        }
        return null;
    }

}
