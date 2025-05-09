package org.example.highjoin.functions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.highjoin.entities.Message;

import java.util.HashMap;
import java.util.HashSet;

public class GroupbyProcess extends KeyedProcessFunction<Object, Message, Message> {

    public MapState<Object, Double> revenue;
    String[] outputAttr = new String[]{"c_custkey", "c_name", "c_acctbal", "n_name", "c_address","c_phone", "c_comment"};

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Object, Double> revenueDescriptor = new MapStateDescriptor<>("revenue", Object.class, Double.class);
        revenue = getRuntimeContext().getMapState(revenueDescriptor);
    }

    @Override
    public void processElement(Message message, KeyedProcessFunction<Object, Message, Message>.Context context, Collector<Message> collector) throws Exception {
        if (isPrint() && (Long) message.getKey() == 523) {
            // 打印message
            printState();
            System.out.println("Groupby receive message: " + message);
            System.out.println("------------------------------");
        }
        if (!revenue.contains(message.keyValue)) {
            revenue.put(message.keyValue, 0.0);
        }
        switch (message.operation) {
            case ADD:
                HashMap<String, Object> attr = message.attr;
                double price = (double) attr.get("l_extendedprice");
                double discount = (double) attr.get("l_discount");
                double newRevenue = revenue.get(message.keyValue) + price * (1 - discount);
                revenue.put(message.keyValue, newRevenue);
                buildOutput(message, collector, attr, newRevenue);
                break;

            case SUBTRACT:
                HashMap<String, Object> attr2 = message.attr;
                double price2 = (double) attr2.get("l_extendedprice");
                double discount2 = (double) attr2.get("l_discount");
                double newRevenue2 = revenue.get(message.keyValue) - price2 * (1 - discount2);
                revenue.put(message.keyValue, newRevenue2);
                buildOutput(message, collector, attr2, newRevenue2);
                break;
        }
    }

    private void buildOutput(Message message, Collector<Message> collector, HashMap<String, Object> attr, double newRevenue) {
        Message outputMsg = new Message();
        HashMap<String, Object> output = new HashMap<>();
        for (String attrName : outputAttr) {
            output.put(attrName, attr.getOrDefault(attrName, null));
        }
        output.put("revenue", newRevenue);
        outputMsg.attr = output;
        outputMsg.keyValue = message.keyValue;
        collector.collect(outputMsg);
    }

    // 遍历打印revenue
    public void printState() throws Exception {
        HashSet<Object> keys = new HashSet<>();
        for (Object key : revenue.keys()) {
            keys.add(key);
        }
        for (Object key : keys) {
            System.out.println("key: " + key + ", value: " + revenue.get(key));
        }

    }

    public boolean isPrint() {
        return false;
    }
}
