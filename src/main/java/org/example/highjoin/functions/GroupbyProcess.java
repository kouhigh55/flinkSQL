package org.example.highjoin.functions;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.highjoin.entities.Message;

public class GroupbyProcess  extends KeyedProcessFunction<Object, Message, Message> {
    @Override
    public void processElement(Message message, KeyedProcessFunction<Object, Message, Message>.Context context, Collector<Message> collector) throws Exception {

    }
}
