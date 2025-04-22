package org.example.highjoin.functions;

import org.apache.flink.configuration.Configuration;
import org.example.highjoin.entities.Message;
import org.example.highjoin.entities.Relation;

import java.text.SimpleDateFormat;
import java.util.Date;

public class OrdersProcess extends CoProcess{
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    private Date start;
    private Date end;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        relation = Relation.getRelationFromName("ORDERS");
        childNum = relation.children.size();
        isRoot = relation.fathers.isEmpty();

        start = format.parse("1993-10-01");
        end = format.parse("1994-01-01");
    }

    @Override
    boolean isValid(Message msg) {
        Date oOrderdate = (Date) msg.attr.get("o_orderdate");
        return oOrderdate.after(start) && oOrderdate.before(end);
    }
}
