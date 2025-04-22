package org.example.highjoin.entities;

import java.util.HashSet;

public enum Relation {
    LINEITEM("lineitem", new String[]{}, new String[]{"ORDERS"},
            new String[]{"l_orderkey", "l_returnflag"},
            "l_orderkey", "l_orderkey"),

    ORDERS("orders", new String[]{"LINEITEM"}, new String[]{"CUSTOMER"},
            new String[]{"o_orderkey", "o_custkey","o_orderdate"},
            "o_custkey", "o_orderkey"),

    CUSTOMER("customer", new String[]{"ORDERS"}, new String[]{"NATION"},
            new String[]{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal","c_comment"},
            "c_nationkey", "c_custkey"),

    NATION("nation", new String[]{"CUSTOMER"}, new String[]{},
            new String[]{"n_nationkey", "n_name"},
            "n_nationkey", "n_nationkey"),
    ;


    public final String name;
    public final HashSet<Relation> fathers = new HashSet<>();
    public final HashSet<Relation> children = new HashSet<>();
    public final HashSet<Relation> attrName = new HashSet<>();
    public final String inputKey;
    public final String outputKey;

    Relation(String name, String inputKey, String outputKey) {
        this.name = name;
        this.inputKey = inputKey;
        this.outputKey = outputKey;
    }

    Relation(String name, String[] fathers, String[] children, String[] attr, String inputKey, String outputKey) {
        this.name = name;
        this.inputKey = inputKey;
        this.outputKey = outputKey;
        for (String father : fathers) {
            this.fathers.add(Relation.valueOf(father));
        }
        for (String child : children) {
            this.children.add(Relation.valueOf(child));
        }
        for (String attribute : attr) {
            this.attrName.add(Relation.valueOf(attribute));
        }
    }
    public static Relation getRelationFromName(String name){
        for (Relation relation : Relation.values()) {
            if (relation.name.equals(name)) {
                return relation;
            }
        }
        return NATION;
    }

}
