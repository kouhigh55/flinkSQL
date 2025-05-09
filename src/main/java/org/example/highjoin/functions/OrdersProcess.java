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
        childNum = relation.children.length;
        isRoot = relation.fathers.length == 0;

        start = format.parse("1993-9-30");
        end = format.parse("1994-01-01");
    }

    @Override
    public boolean isValid(Message msg) {
        Date oOrderdate = (Date) msg.attr.get("o_orderdate");
        return oOrderdate.after(start) && oOrderdate.before(end);// > , <
    }

    @Override
    public boolean isPrint() {
        return false;
    }
}
