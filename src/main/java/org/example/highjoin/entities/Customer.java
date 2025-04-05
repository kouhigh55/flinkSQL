package org.example.highjoin.entities;

import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Data
public class Customer{
    private int cCustkey;

    private String cName;

    private String cAddress;

    private int cNationkey;

    private String cPhone;

    private BigDecimal cAcctbal;

    private String cMktsegment;

    private String cComment;

    private String cDummy;

    private int s;



}