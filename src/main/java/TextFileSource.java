import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

public class TextFileSource {


    public static void main(String[] args) throws Exception {
        String filePath = "E:\\flink-study\\src\\main\\java\\test.txt";
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readFile(format,filePath, FileProcessingMode.PROCESS_CONTINUOUSLY,10000,typeInfo);
//Tuple2 是java的，不是scala
        DataStream<Tuple2<String,Integer>> word =
                dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] tokens = value.toLowerCase().split("\\.");
                        for(String token:tokens) {
                            if (token.length() > 0 ) {
                                out.collect(new Tuple2<String, Integer>(token,1));
                            }
                            }
                        }
                    });
        DataStream<Tuple2<String,Integer>> counts = word.keyBy("f0").sum(1);
                counts.print("输出结果");
        env.execute();
    }
}
