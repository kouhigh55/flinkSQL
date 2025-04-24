package org.example.highjoin.functions;

import org.apache.flink.configuration.Configuration;
import org.example.highjoin.entities.Message;
import org.example.highjoin.entities.Relation;

public class NationProcess extends Process{
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        relation = Relation.getRelationFromName("NATION");
        childNum = relation.children.size();
        isRoot = relation.fathers.isEmpty();
    }

    @Override
    public boolean isValid(Message value) {
        return true;
    }
}
