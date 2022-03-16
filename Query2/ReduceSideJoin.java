package Query2;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.Map;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

/*
    Command line example to run the code:
    bin/hadoop ReduceSideJoin.jar RSJDriver 10 2451146 2452268 path/to/store_sales.dat path/to/store.dat output
*/


public class ReduceSideJoin {
    /*
        The comparator pair contains s_floor_space, ss_net_paid.
        Compare the floor space first, if they are equal, compare the second one.
    */ 
    public static class FloorSpaceNetPaidPair implements WritableComparable<FloorSpaceNetPaidPair>{
        private int s_floor_space;
        private Double ss_net_paid;
    
        public FloorSpaceNetPaidPair() {
            super();
        }
    
        public FloorSpaceNetPaidPair(int s_floor_space, Double ss_net_paid) {
            super();
            this.s_floor_space = s_floor_space;
            this.ss_net_paid = ss_net_paid;
        }
    
        @Override
        public void readFields(DataInput in) throws IOException {
            s_floor_space = in.readInt();
            ss_net_paid = in.readDouble();
        }
    
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(s_floor_space);
            out.writeDouble(ss_net_paid);
        }
    
        @Override
        public int compareTo(FloorSpaceNetPaidPair o) {
            if (this.s_floor_space != o.s_floor_space){
                return this.s_floor_space < o.s_floor_space ? -1 : 1;
            }
            return this.ss_net_paid < o.ss_net_paid ? -1 : 1;
        }

        // for output format ss_store_sk, ss_net_paid, s_floor_space
        @Override
        public String toString(){
            return this.ss_net_paid.toString() + "\t" + this.s_floor_space;
        }

        public int getSsStoreSK() {
            return this.s_floor_space;
        }
    
        public void setSsStoreSK(int s_floor_space) {
            this.s_floor_space = s_floor_space;
        }
    
        public Double getSsNetPaid() {
            return this.ss_net_paid;
        }
    
        public void setSsNetPaid(Double ss_net_paid) {
            this.ss_net_paid = ss_net_paid;
        }
    
    }
    
    public static class StoreMapper extends Mapper<Object, Text, IntWritable, Text>{    
        private IntWritable outKey;
        private Text outValue;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\|", -1);
            int s_store_sk;  // index is 0 in the schema
            String s_floor_space;  // index is 7 in the scehma
            

            if (tokens[0].isEmpty() || tokens[0] == null || tokens[0].trim().isEmpty()) {
                return;
            }
            else {
                s_store_sk = Integer.parseInt(tokens[0]);
            }
            if (tokens[7].isEmpty() || tokens[7] == null || tokens[7].trim().isEmpty()) {
                return;
            }
            else {
                s_floor_space = "store_floor_space\t" + tokens[7];
            }
            outKey = new IntWritable(s_store_sk);
            outValue = new Text(s_floor_space);
            context.write(outKey, outValue);
        }
    }

    public static class StoreSalesMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable outKey;
        private Text outValue;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\|", -1);
            int ss_store_sk;  // index is 7 in the schema
            String ss_net_paid;  // index is 20 in the schema
            
            // Get the input timestamps
            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Reduce side join inputs");
            int start_date = Integer.parseInt(args[1]);
            int end_date = Integer.parseInt(args[2]);

            // Check current data's timestamp
            if (tokens[0].isEmpty() || tokens[0] == null || tokens[0].trim().isEmpty()) {
                return;
            }
            else {
                // If there is no timestamp or the timestamp is not in the specified range, data will not be retrieved
                int temp_timestamp = Integer.parseInt(tokens[0]);
                if (temp_timestamp < start_date || temp_timestamp > end_date) {
                    return;
                }
                else {
                    if (tokens[7].isEmpty() || tokens[7] == null || tokens[7].trim().isEmpty()) {
                        return;
                    }
                    else {
                        ss_store_sk = Integer.parseInt(tokens[7]);
                    }
                    if (tokens[20].isEmpty() || tokens[20] == null || tokens[20].trim().isEmpty()) {
                        return;
                    }
                    else {
                        ss_net_paid = "store_sales_net_paid\t" + tokens[20];
                    }
                }
            }
            outKey = new IntWritable(ss_store_sk);
            outValue = new Text(ss_net_paid);
            context.write(outKey, outValue);
        }
    }

    public static class JoinResultReducer extends Reducer<IntWritable, Text, IntWritable, FloorSpaceNetPaidPair> {
        private IntWritable outKey;
    	private FloorSpaceNetPaidPair outValue = new FloorSpaceNetPaidPair();
        
        private TreeMap<FloorSpaceNetPaidPair, Integer> treeMap = new TreeMap<FloorSpaceNetPaidPair, Integer>(
            new Comparator<FloorSpaceNetPaidPair>() {
            @Override
            public int compare(FloorSpaceNetPaidPair x, FloorSpaceNetPaidPair y) {
                return y.compareTo(x);
            }
        });

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double ss_net_paid_total = 0.0;
            int s_floor_space = 0;
            for (Text value : values) {
                String value_tokens[] = value.toString().split("\t");
                if (value_tokens[0].equals("store_floor_space")) {
                    s_floor_space = Integer.parseInt(value_tokens[1]);
                }
                else if (value_tokens[0].equals("store_sales_net_paid")) {
                    ss_net_paid_total += Double.parseDouble(value_tokens[1]);
                }
            }
            // Some stores will not have any ss_net_paid data in the range
            if (ss_net_paid_total <= 0.0) {
                return;
            }
            FloorSpaceNetPaidPair tempPair = new FloorSpaceNetPaidPair(s_floor_space, ss_net_paid_total);
            treeMap.put(tempPair, key.get());
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Reduce side join inputs");
            int top_k = Integer.parseInt(args[0]);
            int treeSize = treeMap.size();
            if (treeSize < top_k) {
                top_k = treeSize;
            }
            for (int i = 0; i < top_k; i++) {
                Map.Entry<FloorSpaceNetPaidPair, Integer> entry = treeMap.pollFirstEntry();
                outKey = new IntWritable(entry.getValue());
                outValue = entry.getKey();
                context.write(outKey, outValue);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        // Get the parameters from the command line
        conf.setStrings("Reduce side join inputs", args);
        Job job = Job.getInstance(conf, "Reduce Side Join");
        job.setJarByClass(Query2.class);

        // store_sales.dat file path and the mapper class
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, StoreSalesMapper.class);
        // store.dat file path and the mapper class
        MultipleInputs.addInputPath(job, new Path(args[4]), TextInputFormat.class, StoreMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // reducer class
        job.setReducerClass(JoinResultReducer.class);

        // reducer output class
        job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloorSpaceNetPaidPair.class);

        // Output path
        FileOutputFormat.setOutputPath(job, new Path(args[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
