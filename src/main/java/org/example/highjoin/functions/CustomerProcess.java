package org.example.highjoin.functions;

import org.apache.flink.configuration.Configuration;
import org.example.highjoin.entities.Message;
import org.example.highjoin.entities.Relations;

public class CustomerProcess extends Process{


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Relation = "CUSTOMER";
        childNum = Relations.getRelationFromName(Relation).children.size();
    }

    @Override
    boolean isValid(Message value) {
        return true;
    }
}
