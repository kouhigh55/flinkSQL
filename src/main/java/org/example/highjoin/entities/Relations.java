package org.example.highjoin.entities;

import java.util.HashSet;

public enum Relations {
    CUSTOMER("customer", new String[]{"ORDERS"}, new String[]{"NATION"}),
    ORDERS("orders", new String[]{"LINEITEM"}, new String[]{"CUSTOMER"}),
    LINEITEM("lineitem", new String[]{}, new String[]{"ORDERS"}),
    SUPPLIER("supplier", new String[]{}, new String[]{"NATION"}),
    NATION("nation", new String[]{"SUPPLIER","CUSTOMER"}, new String[]{"REGION"}),
    REGION("region", new String[]{"NATION"}, new String[]{});


    public final String name;
    public final HashSet<Relations> fathers = new HashSet<>();
    public final HashSet<Relations> children = new HashSet<>();

    Relations(String name) {
        this.name = name;
    }

    Relations(String customer, String[] fathers, String[] children) {
        this.name = customer;
        for (String father : fathers) {
            this.fathers.add(Relations.valueOf(father));
        }
        for (String child : children) {
            this.children.add(Relations.valueOf(child));
        }

    }
    public Relations getRelationFromName(String name){
        for (Relations relation : Relations.values()) {
            if (relation.name.equals(name)) {
                return relation;
            }
        }
        return null;
    }

}
