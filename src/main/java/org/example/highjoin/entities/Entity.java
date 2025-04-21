package org.example.highjoin.entities;

import java.util.ArrayList;

public abstract class Entity {
    // save assertion and children attributes
    public ArrayList<Entity> otherAttributes = new ArrayList<>();


    // merge two entity
    public void join(Entity child) {
        otherAttributes.add(child);
        otherAttributes.addAll(child.otherAttributes);
    }

}
