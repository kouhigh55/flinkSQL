package org.example.highjoin.entities;

import lombok.Data;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

@Data
public class Customer extends Entity {

    public HashMap<String, Object> data = new HashMap<>();
    
    private int c_custkey;

    private String c_name;

    private String c_address;

    private int c_nationkey;

    private String c_phone;

    private BigDecimal c_acctbal;

    private String c_comment;

}