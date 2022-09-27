import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;
import java.util.List;

public class ReduceTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("123xxxxx", 899, "2018-06"));
        list.add(new Trade("123xxxxx", 699, "2018-06"));
        list.add(new Trade("188xxxxx", 88, "2018-07"));
        list.add(new Trade("188xxxxx", 69, "2018-07"));
        list.add(new Trade("158xxxxx", 100, "2018-06"));
        list.add(new Trade("158xxxxx", 1000, "2018-06"));


        DataStream<Trade> dataSource = env.fromCollection(list);

        DataStream<Trade> resultStream = dataSource.keyBy("cardNum")
                .reduce(new ReduceFunction<Trade>() {
                    @Override
                    public Trade reduce(Trade value1, Trade value2) throws Exception {
                        System.out.println(Thread.currentThread().getName() + "-----" + value1 + ":" + value2);
                        return new Trade(value1.getCardNum(),value1.getTrade()+value2.getTrade(),"-----");
                    }
                });
        resultStream.print("输出结果");
        env.execute("Reduce Template");
    }
}
