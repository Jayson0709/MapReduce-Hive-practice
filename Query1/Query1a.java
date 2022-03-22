package Query1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import javax.lang.model.type.NullType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    Command line structure to run the code:
    bin/hadoop jar your_package.jar main K start_date end_date input_file output_directory
*/


public class Query1a {
    // This data type is intended to preserve
    // the source of the data (Store) when sorting later
    public static class IntDoublePair implements WritableComparable<IntDoublePair> {
        private Double net_paid;
        private Integer store;

        public IntDoublePair(){
            super();
        }

        public IntDoublePair(Integer store, Double net_paid){
            super();
            this.store = store;
            this.net_paid = net_paid;
        }

        public Integer getStore(){
            return store;
        }

        public Double getNetPaid(){
            return net_paid;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(store);
            out.writeDouble(net_paid);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            store = in.readInt();
            net_paid = in.readDouble();
        }

        @Override
        public int compareTo(IntDoublePair o) {
            if (this.net_paid != o.net_paid) {
                return this.net_paid < o.net_paid ? -1 : 1;
            }
            return (this.store < o.store ? -1 : (this.net_paid == o.net_paid ? 0 : 1));
        }

    }

    public static class HighestTotalNetPaidMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            // Get the input timestamps
            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Query1a inputs");

            // get data from the command line
            int start_date = Integer.parseInt(args[1]);
            int end_date = Integer.parseInt(args[2]);

            // the data from the driver
            String data = value.toString();
            String[] data_array = data.split("\\|", -1);

            int ss_store_sk = 0;
            Double ss_net_paid = 0D;

            // Determine the time, if it matches the time then bring in
            if(data_array[0].isEmpty()){
                return;
            }
            int tmp_time = Integer.parseInt(data_array[0]);

            if (tmp_time < start_date || tmp_time > end_date){
                return;
            }

            // Determine if there is content in the data and bring it in if there is
            if(data_array[7].isEmpty() || data_array[20].isEmpty()){
                return;
            } else {
                ss_store_sk = Integer.parseInt(data_array[7]);
                ss_net_paid = Double.parseDouble(data_array[20]);
            }
            context.write(new IntWritable(ss_store_sk), new DoubleWritable(ss_net_paid));
        }
    }

    public static class HighestTotalNetPaidReducer  extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        private TreeMap<IntDoublePair, NullType> TopKMap = new TreeMap<IntDoublePair, NullType>(new Comparator<IntDoublePair>() {
            @Override
            // rewrite the compare function
            public int compare(IntDoublePair o1, IntDoublePair o2){
                return o2.getNetPaid().compareTo(o1.getNetPaid());
            }
        });

        public void reduce(IntWritable key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
            Double local_sum = 0D;
            for(DoubleWritable v:value){
                local_sum += v.get();
            }

            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Query1a inputs");
            int topK = Integer.parseInt(args[0]);

            TopKMap.put(new IntDoublePair(key.get(), local_sum), null);

            if (TopKMap.size() > topK){
                TopKMap.pollLastEntry();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException{
            for(Map.Entry<IntDoublePair,NullType> entry: TopKMap.entrySet()){
                DoubleWritable sum = new DoubleWritable(entry.getKey().getNetPaid());
                IntWritable store = new IntWritable(entry.getKey().getStore());
                context.write(store, sum);
            }
        }
    }

    public static class HighestTotalNetPaidCombiner  extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
            Double sum = 0D;
            for(DoubleWritable v:value){
                sum += v.get();
            }

            context.write(key, new DoubleWritable(sum));
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.setStrings("Query1a inputs", args);
        Job job = Job.getInstance(conf, "Query 1-a");
        job.setJarByClass(Query1a.class);

        job.setMapperClass(HighestTotalNetPaidMapper.class);
        job.setCombinerClass(HighestTotalNetPaidCombiner.class);
        job.setReducerClass(HighestTotalNetPaidReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

