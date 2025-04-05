package org.example.highjoin.entities;

import lombok.Data;

@Data
public class Nation {
    private int nNationkey;

    private String nName;

    private int nRegionkey;

    private String nComment;

    private String nDummy;
    private transient int s;
}