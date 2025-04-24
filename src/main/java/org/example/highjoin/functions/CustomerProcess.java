package org.example.highjoin.functions;

import org.apache.flink.configuration.Configuration;
import org.example.highjoin.entities.Message;
import org.example.highjoin.entities.Relation;

public class CustomerProcess extends CoProcess{


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        relation = Relation.getRelationFromName("CUSTOMER");
        childNum = relation.children.size();
        isRoot = relation.fathers.isEmpty();
    }

    @Override
    public boolean isValid(Message value) {
        return true;
    }
}
