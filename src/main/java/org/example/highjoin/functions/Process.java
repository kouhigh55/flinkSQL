package org.example.highjoin.functions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.highjoin.entities.Message;
import org.example.highjoin.entities.Relations;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Process extends KeyedProcessFunction<Object, Message, Message> {
    // key, array(inserted data)
    MapState<Object, ArrayList<HashMap<String, Object>>> index;
    // key, cnt
    MapState<Object, Integer> cnt;
    // key, assertion key or need joined children data
    MapState<Object, HashMap<String, Object>> childAttr;

    String Relation;

    int childNum;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Object, ArrayList> indexDescriptor = new MapStateDescriptor<>("index", Object.class, ArrayList.class);
        MapStateDescriptor<Object, Integer> cntDescriptor = new MapStateDescriptor<>("cnt", Object.class, Integer.class);
        MapStateDescriptor<Object, Object> childAttrDescriptor = new MapStateDescriptor<>("childAttr", Object.class, Object.class);
    }


    @Override
    public void processElement(Message value, KeyedProcessFunction<Object, Message, Message>.Context ctx, Collector<Message> out) throws Exception {

    }

    // satisfy where condition
    abstract boolean isValid(Message value);
}
