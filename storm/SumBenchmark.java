package storm;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.streams.Pair;
import org.apache.storm.generated.*;
import org.apache.storm.spout.RawScheme;
import org.apache.storm.sql.runtime.datasource.socket.spout.SocketSpout;
import java.nio.charset.StandardCharsets;
import org.apache.storm.utils.Utils;
import java.util.Arrays;

// import storm.SocketSpout;

public class SumBenchmark {
    public static Pair<String, Pair<String, String>> processInput(Tuple x) {
        byte[] b = (byte[]) x.getValueByField("bytes");
        String str = new String(b, StandardCharsets.UTF_8);
        String[] parts = str.split("\t");

        return Pair.of(parts[1], Pair.of(parts[2], parts[3]));
    }

    public static void main(String[] args) {
        if(args.length < 3) { System.out.println("Must supply input_ip, input_port, mongo_ip, and num_workers"); }
        String input_IP = args[0];
        String input_port = args[1];

        Integer num_workers = Integer.parseInt(args[3]);
        Integer gen_rate = Integer.parseInt(args[4]);
        String NTP_IP = "";
        if(args.length > 5) { NTP_IP = args[5]; }
        
        // Socket spout to get input tuples
        // JsonScheme inputScheme = new JsonScheme(Arrays.asList("gem", "price", "event_time"));
        RawScheme inputScheme = new RawScheme();
        // SocketSpout sSpout = new SocketSpout(inputScheme, input_IP, Integer.parseInt(input_port));
        SocketSpout sSpout = new SocketSpout(inputScheme, input_IP, Integer.parseInt(input_port));

        // Mongo bolt to store the results
        // String mongo_addr = "mongodb://storm:test@" + mongo_IP + ":27017/results?authSource=admin";
        // SimpleMongoMapper mongoMapper = new SimpleMongoMapper().withFields("GemID", "aggregate", "latency", "time");
        // MongoInsertBolt mongoBolt = new MongoInsertBolt(mongo_addr, "aggregation", mongoMapper);

        // Build the topology (stream api)
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(sSpout, num_workers * 8) // Get tuples from TCP socket
            // Window the input into (8s, 4s) windows
            .window(SlidingWindows.of( // Window times in millis
                Duration.of((int) Math.round(1000 * 8.0 / num_workers)), 
                Duration.of((int) Math.round(1000 * 4.0 / num_workers))
            ))
            .map(x -> processInput(x)).print();
            // Map to key-value pair with the GemID as key, and an AggregationResult as value
            // .mapToPair(x -> Pair.of(x[1]), new AggregationResult(x))
            // // Aggregate the window by key
            // .aggregateByKey(new SumAggregator())
            // // Insert the results into the mongo database
            // .to(mongoBolt);

        Config config = new Config();
        config.setNumWorkers(num_workers); // Number of supervisors to work on topology
        config.setMaxSpoutPending(Math.round(4 * 4 * gen_rate)); // Maximum # unacked tuples
        try {
            StormSubmitter.submitTopologyWithProgressBar("agsum", config, builder.build());
        } catch (Exception e) {
            System.out.println("ERR");
        }
        // catch(AlreadyAliveException e) { System.out.println("Already alive"); }
        // catch(InvalidTopologyException e) { System.out.println("Invalid topolgy"); }
        // catch(AuthorizationException e) { System.out.println("Auth problem"); }
    }
}
