package org.example.highjoin.entities;

import lombok.Data;

import java.math.BigDecimal;
import java.sql.Date;

@Data
public class LineItem extends Entity{
    private int lOrderkey;
    private int lPartkey;
    private int lSuppkey;
    private int lLinenumber;
    private BigDecimal lQuantity;
    private BigDecimal lExtendedprice;
    private BigDecimal lDiscount;
    private BigDecimal lTax;
    private char lReturnflag;
    private char lLinestatus;
    private Date lShipdate;
    private Date lCommitdate;
    private Date lReceiptdate;
    private String lShipinstruct;
    private String lShipmode;
    private String lComment;
    private String lDummy;
}