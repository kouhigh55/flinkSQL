package org.example.highjoin.functions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.highjoin.entities.Message;
import org.example.highjoin.entities.Relation;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Process extends KeyedProcessFunction<Object, Message, Message> {
    // key, array(inserted data)
    MapState<Object, ArrayList<HashMap<String, Object>>> index;
    // key, cnt
    MapState<Object, Integer> cnt;
    // key, assertion key or need joined children data
    MapState<Object, HashMap<String, Object>> childAttr;

    Relation relation;

    int childNum;

    boolean isRoot = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Object, ArrayList<HashMap<String, Object>>> indexDescriptor = new MapStateDescriptor<>("index",
                TypeInformation.of(new TypeHint<Object>() {}),
                TypeInformation.of(new TypeHint<ArrayList<HashMap<String, Object>>>() {}));
        MapStateDescriptor<Object, Integer> cntDescriptor = new MapStateDescriptor<>("cnt", Object.class, Integer.class);
        MapStateDescriptor<Object, HashMap<String, Object>> childAttrDescriptor = new MapStateDescriptor<>("childAttr",
                TypeInformation.of(new TypeHint<Object>() {}),
                TypeInformation.of(new TypeHint<HashMap<String, Object>>() {}));
        index = getRuntimeContext().getMapState(indexDescriptor);
        cnt = getRuntimeContext().getMapState(cntDescriptor);
        childAttr = getRuntimeContext().getMapState(childAttrDescriptor);
    }


    @Override
    public void processElement(Message value, KeyedProcessFunction<Object, Message, Message>.Context ctx, Collector<Message> out) throws Exception {
        myProcess(value, ctx,null, out);
    }

    // satisfy where condition
    abstract boolean isValid(Message value);

    public static void myProcess(Message value, KeyedProcessFunction<Object, Message, Message>.Context ctx, KeyedCoProcessFunction<Object, Message, Message, Message>.Context ctxCor, Collector<Message> out) throws Exception {




        out.collect(value);
    }

}
