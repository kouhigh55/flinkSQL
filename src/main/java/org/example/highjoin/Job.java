package org.example.highjoin;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.highjoin.entities.Message;
import org.example.highjoin.entities.Operation;
import org.example.highjoin.entities.Relation;
import org.example.highjoin.functions.CustomerProcess;
import org.example.highjoin.functions.LineItemProcess;
import org.example.highjoin.functions.NationProcess;
import org.example.highjoin.functions.OrdersProcess;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class Job {
    private static final OutputTag<Message> lineitemTag = new OutputTag<>("lineitem");
    private static final OutputTag<Message> ordersTag = new OutputTag<>("orders");
    private static final OutputTag<Message> customerTag = new OutputTag<>("customer");
    private static final OutputTag<Message> nationTag = new OutputTag<>("nation");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();

        String inputPath = "D:\\javacode\\Cquirrel-Frontend-master\\DemoTools\\DataGenerator\\output_data.csv";
        String outputPath = "D:\\javacode\\Cquirrel-Frontend-master\\DemoTools\\DataGenerator\\q10result.csv";

        SingleOutputStreamOperator<Message> inputStream = getStream(env, inputPath);
        SideOutputDataStream<Message> orders = inputStream.getSideOutput(ordersTag);
        SideOutputDataStream<Message> lineitem = inputStream.getSideOutput(lineitemTag);
        SideOutputDataStream<Message> nation = inputStream.getSideOutput(nationTag);
        SideOutputDataStream<Message> customer = inputStream.getSideOutput(customerTag);

        SingleOutputStreamOperator<Message> nationS = nation.keyBy(i -> i.getKey())
                .process(new NationProcess());

        SingleOutputStreamOperator<Message> customerS = nationS.connect(customer)
                .keyBy(i -> i.getKey(), i -> i.getKey())
                .process(new CustomerProcess());

        SingleOutputStreamOperator<Message> ordersS = customerS.connect(orders)
                .keyBy(i -> i.getKey(), i -> i.getKey())
                .process(new OrdersProcess());

        SingleOutputStreamOperator<Message> lineitemS = ordersS.connect(lineitem)
                .keyBy(i -> i.getKey(), i -> i.getKey())
                .process(new LineItemProcess());

        lineitemS.keyBy(i -> i.getKey())
                .process(new Q10AggregateProcessFunction())
                .map(x -> x.getAttributes()[3] + ", " + x.getAttributes()[4] + ", " + x.getAttributes()[5])
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
    }

    private static SingleOutputStreamOperator<Message> getStream(StreamExecutionEnvironment env, String dataPath) {
        DataStream<String> data = env.readTextFile(dataPath).setParallelism(1);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        SingleOutputStreamOperator<Message> process = data.process(new ProcessFunction<String, Message>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Message> out) throws Exception {
                String header = value.substring(0, 3);
                String[] cells = value.substring(3).split("\\|");
                Operation action;
                Relation relation;
                HashMap<String, Object> attr = new HashMap<>();
                ;
                // tables.sql
                switch (header) {
                    case "+LI":
                        action = Operation.INSERT;
                        relation = Relation.LINEITEM;
                        attr.put("l_orderkey", Integer.parseInt(cells[0]));
                        attr.put("l_returnflag", cells[8]);
                        ctx.output(lineitemTag, new Message(attr, action, relation, Integer.parseInt(cells[0])));
                        break;
                    case "-LI":
                        action = Operation.DELETE;
                        relation = Relation.LINEITEM;
                        attr.put("l_orderkey", Integer.parseInt(cells[0]));
                        attr.put("l_returnflag", cells[8]);
                        ctx.output(lineitemTag, new Message(attr, action, relation, Integer.parseInt(cells[0])));
                        break;
                    case "+OR":
                        action = Operation.INSERT;
                        relation = Relation.ORDERS;
                        attr.put("o_orderkey", Integer.parseInt(cells[0]));
                        attr.put("o_custkey", Integer.parseInt(cells[1]));
                        attr.put("o_orderdate", format.parse(cells[4]));
                        ctx.output(ordersTag, new Message(attr, action, relation, Integer.parseInt(cells[1])));
                        break;
                    case "-OR":
                        action = Operation.DELETE;
                        relation = Relation.ORDERS;
                        attr.put("o_orderkey", Integer.parseInt(cells[0]));
                        attr.put("o_custkey", Integer.parseInt(cells[1]));
                        Date parse = format.parse(cells[4]);
                        attr.put("o_orderdate", format.parse(cells[4]));
                        ctx.output(ordersTag, new Message(attr, action, relation, Integer.parseInt(cells[1])));
                        break;
                    case "+CU":

                        action = Operation.INSERT;
                        relation = Relation.CUSTOMER;
                        attr.put("c_custkey", Integer.parseInt(cells[0]));
                        attr.put("c_name", cells[1]);
                        attr.put("c_address", cells[2]);
                        attr.put("c_nationkey", Integer.parseInt(cells[3]));
                        attr.put("c_phone", cells[4]);
                        attr.put("c_acctbal", Double.parseDouble(cells[5]));
                        attr.put("c_comment", cells[7]);
                        ctx.output(customerTag, new Message(attr, action, relation, Integer.parseInt(cells[3])));
                        break;
                    case "-CU":
                        action = Operation.DELETE;
                        relation = Relation.CUSTOMER;
                        attr.put("c_custkey", Integer.parseInt(cells[0]));
                        attr.put("c_name", cells[1]);
                        attr.put("c_address", cells[2]);
                        attr.put("c_nationkey", Integer.parseInt(cells[3]));
                        attr.put("c_phone", cells[4]);
                        attr.put("c_acctbal", Double.parseDouble(cells[5]));
                        attr.put("c_comment", cells[7]);
                        ctx.output(customerTag, new Message(attr, action, relation, Integer.parseInt(cells[3])));
                        break;
                    case "+NA":
                        action = Operation.INSERT;
                        relation = Relation.NATION;
                        attr.put("n_nationkey", Integer.parseInt(cells[0]));
                        attr.put("n_name", cells[1]);
                        ctx.output(nationTag, new Message(attr, action, relation, Integer.parseInt(cells[0])));
                        break;
                    case "-NA":
                        action = Operation.DELETE;
                        relation = Relation.NATION;
                        attr.put("n_nationkey", Integer.parseInt(cells[0]));
                        attr.put("n_name", cells[1]);
                        ctx.output(nationTag, new Message(attr, action, relation, Integer.parseInt(cells[0])));
                        break;
                    default:
                        out.collect(new Message(attr, Operation.INSERT, Relation.getRelationFromName(header), Integer.parseInt(cells[0])));
                        break;
                }
            }
        }).setParallelism(1);
        return process;
    }
}