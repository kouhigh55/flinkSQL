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
import org.example.highjoin.entities.Operation;
import org.example.highjoin.entities.Relation;

import java.util.ArrayList;
import java.util.HashMap;

import static org.example.highjoin.entities.Operation.SETALIVE;
import static org.example.highjoin.entities.Operation.SETDEAD;

public class CoProcess extends KeyedCoProcessFunction<Object, Message, Message, Message> {
    // key, array(inserted data with input key value)
    MapState<Object, ArrayList<Message>> index;
    // key, cnt(number of alived children with input key value)
    MapState<Object, Integer> cnt;
    // key, assertion key or need joined children data (save children data, cause state cannot be visited in diff func)
    MapState<Object, HashMap<String, Object>> childAttr;

    Relation relation;

    int childNum;

    boolean isRoot = false;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Object, ArrayList<Message>> indexDescriptor = new MapStateDescriptor<>("index",
                TypeInformation.of(new TypeHint<Object>() {
                }),
                TypeInformation.of(new TypeHint<ArrayList<Message>>() {
                }));
        MapStateDescriptor<Object, Integer> cntDescriptor = new MapStateDescriptor<>("cnt", Object.class, Integer.class);
        MapStateDescriptor<Object, HashMap<String, Object>> childAttrDescriptor = new MapStateDescriptor<>("childAttr",
                TypeInformation.of(new TypeHint<Object>() {
                }),
                TypeInformation.of(new TypeHint<HashMap<String, Object>>() {
                }));
        index = getRuntimeContext().getMapState(indexDescriptor);
        cnt = getRuntimeContext().getMapState(cntDescriptor);
        childAttr = getRuntimeContext().getMapState(childAttrDescriptor);


    }

    @Override
    public void processElement1(Message message, KeyedCoProcessFunction<Object, Message, Message, Message>.Context context, Collector<Message> collector) throws Exception {
        myProcess(message, collector);
    }

    @Override
    public void processElement2(Message message, KeyedCoProcessFunction<Object, Message, Message, Message>.Context context, Collector<Message> collector) throws Exception {
        myProcess(message, collector);
    }

    public void myProcess(Message message, Collector<Message> out) throws Exception {
        if (!index.contains(message.keyValue)) {
            index.put(message.keyValue, new ArrayList<>());
        }
        if (!cnt.contains(message.keyValue)) {
            cnt.put(message.keyValue, 0);
        }
        if (!childAttr.contains(message.keyValue)) {
            childAttr.put(message.keyValue, new HashMap<>());
        }
        ArrayList<Message> savedData;

        if (message.targetRelation != relation) {
            return;
        }

        switch (message.operation) {
            case INSERT:
                if (!isValid(message)) {
                    return;
                }
                //save into index
                savedData = index.get(message.keyValue);
                savedData.add(message.clone(SETALIVE, message.targetRelation, message.keyValue));
                // is alive
                if (cnt.get(message.keyValue) < childNum) {
                    return;
                }
                // join childAttr
                joinChildAttr(message);
                // does not affect saved data
                message = message.clone(message.operation, message.targetRelation, message.attr.get(relation.outputKey));
                if (isRoot) {
                    message.operation = Operation.ADD;
                    out.collect(message);
                } else {
                    message.operation = SETALIVE;
                    // send to fathers
                    for (String father : relation.fathers) {
                        message.targetRelation = Relation.getRelationFromName(father);
                        out.collect(message);
                    }
                }
                break;

            case DELETE:
                if (!isValid(message)) {
                    return;
                }
                //delete from index
                savedData = index.get(message.keyValue);
                boolean isRemoved = savedData.remove(message.clone(SETALIVE, message.targetRelation, message.keyValue));
                if (!isRemoved) {
                    return;
                }
                // is alive
                if (cnt.get(message.keyValue) < childNum) {
                    return;
                }
                // join childAttr
                joinChildAttr(message);
                // does not affect saved data
                message = message.clone(message.operation, message.targetRelation, message.attr.get(relation.outputKey));
                if (isRoot) {
                    message.operation = Operation.SUBTRACT;
                    out.collect(message);
                } else {
                    message.operation = SETDEAD;
                    // send to fathers
                    for (String father : relation.fathers) {
                        message.targetRelation = Relation.getRelationFromName(father);
                        out.collect(message);
                    }
                }
                break;

            case SETALIVE:
                cnt.put(message.keyValue, cnt.get(message.keyValue) + 1);
                childAttr.get(message.keyValue).putAll(message.attr);
                if (cnt.get(message.keyValue) < childNum) {
                    return;
                }
                savedData = index.get(message.keyValue);
                for (Message msg : savedData) {
                    // join childAttr
                    joinChildAttr(msg);
                    // does not affect saved data
                    msg = msg.clone(msg.operation, msg.targetRelation, msg.attr.get(relation.outputKey));
                    if (isRoot) {
                        msg.operation = Operation.ADD;
                        out.collect(message);
                    } else {
                        msg.operation = SETALIVE;
                        // send to fathers
                        for (String father : relation.fathers) {
                            message.targetRelation = Relation.getRelationFromName(father);
                            out.collect(message);
                        }
                    }
                }
                break;
            case SETDEAD:
                cnt.put(message.keyValue, cnt.get(message.keyValue) - 1);
                if (cnt.get(message.keyValue) < childNum - 1) {
                    return;
                }
                savedData = index.get(message.keyValue);
                for (Message msg : savedData) {
                    // join childAttr
                    joinChildAttr(msg);
                    // does not affect saved data
                    msg = msg.clone(msg.operation, msg.targetRelation, msg.attr.get(relation.outputKey));
                    if (isRoot) {
                        msg.operation = Operation.SUBTRACT;
                        out.collect(message);
                    } else {
                        msg.operation = SETDEAD;
                        // send to fathers
                        for (String father : relation.fathers) {
                            message.targetRelation = Relation.getRelationFromName(father);
                            out.collect(message);
                        }
                    }
                }
                childAttr.get(message.keyValue).clear();
                break;
        }

    }

    // satisfy where condition
    public boolean isValid(Message value) {
        return true;
    }

    public void joinChildAttr(Message message) throws Exception {
        // join childAttr
        HashMap<String, Object> stringObjectHashMap = null;
        if (childAttr.contains(message.keyValue)) {
            stringObjectHashMap = childAttr.get(message.keyValue);
        }
        if (stringObjectHashMap != null && !stringObjectHashMap.isEmpty()) {
            message.attr.putAll(stringObjectHashMap);
        }
    }
}
