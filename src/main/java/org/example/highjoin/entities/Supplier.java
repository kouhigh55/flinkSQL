package org.example.highjoin.entities;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Supplier {
    private int sSuppkey;
    private String sName;
    private String sAddress;
    private int sNationkey;
    private String sPhone;
    private BigDecimal sAcctbal;
    private String sComment;
    private String sDummy;
    private int s;
}