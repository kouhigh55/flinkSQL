package org.example.highjoin.entities;

import java.util.HashSet;

public enum Relations {
    LINEITEM("lineitem", new String[]{}, new String[]{"ORDERS"},
            new String[]{"l_orderkey", "l_returnflag"}),
    ORDERS("orders", new String[]{"LINEITEM"}, new String[]{"CUSTOMER"},
            new String[]{"o_orderkey", "o_custkey","o_orderdate"}),
    CUSTOMER("customer", new String[]{"ORDERS"}, new String[]{"NATION"},
            new String[]{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal","c_comment"}),
    NATION("nation", new String[]{"CUSTOMER"}, new String[]{},
            new String[]{"n_nationkey", "n_name"});


    public final String name;
    public final HashSet<Relations> fathers = new HashSet<>();
    public final HashSet<Relations> children = new HashSet<>();
    public final HashSet<Relations> attrName = new HashSet<>();

    Relations(String name) {
        this.name = name;
    }

    Relations(String customer, String[] fathers, String[] children, String[] attr) {
        this.name = customer;
        for (String father : fathers) {
            this.fathers.add(Relations.valueOf(father));
        }
        for (String child : children) {
            this.children.add(Relations.valueOf(child));
        }
        for (String attribute : attr) {
            this.attrName.add(Relations.valueOf(attribute));
        }
    }
    public static Relations getRelationFromName(String name){
        for (Relations relation : Relations.values()) {
            if (relation.name.equals(name)) {
                return relation;
            }
        }
        return NATION;
    }

}
