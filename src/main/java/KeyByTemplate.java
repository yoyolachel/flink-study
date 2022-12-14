import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class KeyByTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        list.add(new Tuple2<Integer, Integer>(1,11));
        list.add(new Tuple2<Integer, Integer>(1, 22));
        list.add(new Tuple2<Integer, Integer>(3, 33));
        list.add(new Tuple2<Integer, Integer>(5, 55));

        DataStream<Tuple2<Integer, Integer>> dataStream = env.fromCollection(list);

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = dataStream.keyBy(0);
        keyedStream.print("输出结果");
        env.execute("KeyBy Template");
    }
}
