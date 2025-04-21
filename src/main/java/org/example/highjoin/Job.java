package org.example.highjoin;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.highjoin.entities.Entity;
import org.example.highjoin.entities.LineItem;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Job {
    private static final OutputTag<Entity> lineitemTag = new OutputTag<>("lineitem");
    private static final OutputTag<Entity> ordersTag = new OutputTag<>("orders");
    private static final OutputTag<Entity> customerTag = new OutputTag<>("customer");
    private static final OutputTag<Entity> nationTag = new OutputTag<>("nation");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();

        String inputPath = "D:\\javacode\\Cquirrel-Frontend-master\\DemoTools\\DataGenerator\\output_data.csv";
        String outputPath = "D:\\javacode\\Cquirrel-Frontend-master\\DemoTools\\DataGenerator\\q10result.csv";

        DataStream<Entity> inputStream = getStream(env, inputPath);
        DataStream<Entity> orders = inputStream.getSideOutput(ordersTag);
        DataStream<Entity> lineitem = inputStream.getSideOutput(lineitemTag);
        DataStream<Entity> nation = inputStream.getSideOutput(nationTag);
        DataStream<Entity> customer = inputStream.getSideOutput(customerTag);

        DataStream<Entity> nationS = nation.keyBy(i -> i.getKey())
                .process(new Q10NationProcessFunction());

        DataStream<Entity> customerS = nationS.connect(customer)
                .keyBy(i -> i.getKey(), i -> i.getKey())
                .process(new Q10CustomerProcessFunction());

        DataStream<Entity> ordersS = customerS.connect(orders)
                .keyBy(i -> i.getKey(), i -> i.getKey())
                .process(new Q10OrdersProcessFunction());

        DataStream<Entity> lineitemS = ordersS.connect(lineitem)
                .keyBy(i -> i.getKey(), i -> i.getKey())
                .process(new Q10LineitemProcessFunction());

        lineitemS.keyBy(i -> i.getKey())
                .process(new Q10AggregateProcessFunction())
                .map(x -> x.getAttributes()[3] + ", " + x.getAttributes()[4] + ", " + x.getAttributes()[5])
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
    }

    private static DataStream<Entity> getStream(StreamExecutionEnvironment env, String dataPath) {
        DataStream<String> data = env.readTextFile(dataPath).setParallelism(1);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final long[] cnt = {0};

        return data.process(new ProcessFunction<String, Entity>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Entity> out) throws Exception {
                String header = value.substring(0, 3);
                String[] cells = value.substring(3).split("\\|");
                String relation = "";
                String action = "";

                switch (header) {
                    case "+LI":
                        action = "Insert";
                        relation = "lineitem";
                        cnt[0]++;
                        ctx.output(lineitemTag, new LineItem(Long.parseLong(cells[0]),
                                new Object[]{Integer.parseInt(cells[3]), Long.parseLong(cells[0]), Double.parseDouble(cells[5]), cells[8], cells[15], Double.parseDouble(cells[6])},
                                new String[]{"LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE", "L_RETURNFLAG", "L_COMMENT", "L_DISCOUNT"},
                                cnt[0]
                        ));
                        break;
                    case "-LI":
                        action = "Delete";
                        relation = "lineitem";
                        cnt[0]++;
                        ctx.output(lineitemTag, new Entity(
                                relation, action, Long.parseLong(cells[0]),
                                new Object[]{Integer.parseInt(cells[3]), Long.parseLong(cells[0]), Double.parseDouble(cells[5]), cells[8], cells[15], Double.parseDouble(cells[6])},
                                new String[]{"LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE", "L_RETURNFLAG", "L_COMMENT", "L_DISCOUNT"},
                                cnt[0]
                        ));
                        break;
                    case "+OR":
                        action = "Insert";
                        relation = "orders";
                        cnt[0]++;
                        ctx.output(ordersTag, new Entity(
                                relation, action, Long.parseLong(cells[1]),
                                new Object[]{Long.parseLong(cells[1]), Long.parseLong(cells[0]), format.parse(cells[4]), cells[8]},
                                new String[]{"CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_COMMENT"},
                                cnt[0]
                        ));
                        break;
                    case "-OR":
                        action = "Delete";
                        relation = "orders";
                        cnt[0]++;
                        ctx.output(ordersTag, new Entity(
                                relation, action, Long.parseLong(cells[1]),
                                new Object[]{Long.parseLong(cells[1]), Long.parseLong(cells[0]), format.parse(cells[4]), cells[8]},
                                new String[]{"CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_COMMENT"},
                                cnt[0]
                        ));
                        break;
                    case "+CU":
                        action = "Insert";
                        relation = "customer";
                        cnt[0]++;
                        ctx.output(customerTag, new Entity(
                                relation, action, Long.parseLong(cells[3]),
                                new Object[]{Long.parseLong(cells[0]), Long.parseLong(cells[3]), cells[1], Double.parseDouble(cells[5]), cells[4], cells[2], cells[7]},
                                new String[]{"CUSTKEY", "NATIONKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "C_ADDRESS", "C_COMMENT"},
                                cnt[0]
                        ));
                        break;
                    case "-CU":
                        action = "Delete";
                        relation = "customer";
                        cnt[0]++;
                        ctx.output(customerTag, new Entity(
                                relation, action, Long.parseLong(cells[3]),
                                new Object[]{Long.parseLong(cells[0]), Long.parseLong(cells[3]), cells[1], Double.parseDouble(cells[5]), cells[4], cells[2], cells[7]},
                                new String[]{"CUSTKEY", "NATIONKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "C_ADDRESS", "C_COMMENT"},
                                cnt[0]
                        ));
                        break;
                    case "+NA":
                        action = "Insert";
                        relation = "nation";
                        cnt[0]++;
                        ctx.output(nationTag, new Entity(
                                relation, action, Long.parseLong(cells[0]),
                                new Object[]{Long.parseLong(cells[0]), cells[1], cells[3]},
                                new String[]{"NATIONKEY", "N_NAME", "N_COMMENT"},
                                cnt[0]
                        ));
                        break;
                    case "-NA":
                        action = "Delete";
                        relation = "nation";
                        cnt[0]++;
                        ctx.output(nationTag, new Entity(
                                relation, action, Long.parseLong(cells[0]),
                                new Object[]{Long.parseLong(cells[0]), cells[1], cells[3]},
                                new String[]{"NATIONKEY", "N_NAME", "N_COMMENT"},
                                cnt[0]
                        ));
                        break;
                    default:
                        out.collect(new Entity("", "", 0, new Object[]{}, new String[]{}, 0));
                        break;
                }
            }
        }).setParallelism(1);
    }
}