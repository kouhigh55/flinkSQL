package org.example.highjoin.functions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.highjoin.entities.Relations;

public class Process extends KeyedProcessFunction<Relations, Tuple2<String, Object>, Object> {
    MapState <Integer, Object> index;
    MapState <Integer, Object> indexL;
    MapState <Integer, Object> indexN;
    // only one child in this SQL
    MapState <Integer, Object> indexChild1;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        index = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Integer.class, Object.class));
        indexL = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Integer.class, Object.class));
        indexN = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Integer.class, Object.class));
        indexChild1 = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Integer.class, Object.class));
    }

    @Override
    public void processElement(Tuple2<String, Object> value, KeyedProcessFunction<Relations, Tuple2<String, Object>, Object>.Context ctx, Collector<Object> out) throws Exception {
        // insert, insert-update, delete, delete-update
        String f0 = value.f0;
        switch (f0){
            case "insert":

                break;
            case "insert-update":
                break;
            case "delete":
                break;
            case "delete-update":
                break;
            default:
                break;
        }

    }
}
