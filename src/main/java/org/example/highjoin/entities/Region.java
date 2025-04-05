package org.example.highjoin.entities;

import lombok.Data;

@Data
public class Region {
    private int rRegionkey;
    private String rName;
    private String rComment;
    private String rDummy;
    private transient int s;
}