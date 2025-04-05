package org.example.highjoin.entities;

import lombok.Data;

import java.math.BigDecimal;
import java.sql.Date;

@Data
public class Orders {
    private int oOrderkey;
    private int oCustkey;
    private char oOrderstatus;
    private BigDecimal oTotalprice;
    private Date oOrderdate;
    private String oOrderpriority;
    private String oClerk;
    private int oShippriority;
    private String oComment;
    private String oDummy;
    private transient int s;
}