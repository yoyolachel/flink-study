import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ProjectTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<String, Integer, String>> list = new ArrayList<Tuple3<String, Integer, String>>();
        list.add(new Tuple3<String, Integer, String>("185xxxxxx", 899, "周一"));
        list.add(new Tuple3<String, Integer, String>("155xxxxxx", 1199, "周二"));
        list.add(new Tuple3<String, Integer, String>("185xxxxxx", 19, "周三"));

        DataStream<Tuple3<String, Integer, String>> streamSource = env.fromCollection(list);
        DataStream<Tuple2<String, String>> result = streamSource.project(2, 0);

        result.print("输出结果");
        env.execute("Project Template");

    }
}
