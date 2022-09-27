import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class AggregationsTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("123xxxxx", 899, "2018-06"));
        list.add(new Trade("123xxxxx", 699, "2018-06"));
        list.add(new Trade("188xxxxx", 88, "2018-07"));
        list.add(new Trade("188xxxxx", 69, "2018-07"));
        list.add(new Trade("158xxxxx", 100, "2018-06"));
        list.add(new Trade("158xxxxx", 1000, "2018-06"));


        DataStream<Trade> streamSource = env.fromCollection(list);
        KeyedStream<Trade, Tuple> keyedStream = streamSource.keyBy("cardNum");
        keyedStream.sum("trade").print("sum");
        keyedStream.min("trade").print("min");
        keyedStream.minBy("trade").print("minBy");
        env.execute("Aggregations Template");
    }
}
