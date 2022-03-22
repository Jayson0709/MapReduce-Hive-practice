package Query1;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Collections;

import javax.lang.model.type.NullType;
import javax.naming.Context;
import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Command line structure to run the code
 * bin/hadoop jar your_package.jar main K start_date end_date input_file output_directory 
 */


public class Query1b {
    public static class IdQuantityPair implements WritableComparable<IdQuantityPair> {
        private Integer id;
        private Integer quantity;

        public IdQuantityPair() {
            super();
        }

        public IdQuantityPair(Integer id, Integer quantity) {
            super();
            this.id = id;
            this.quantity = quantity;
        }

        public Integer getId() {
            return id;
        }

        public Integer getQuantity() {
            return quantity;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.id);
            out.writeInt(this.quantity);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = in.readInt();
            this.quantity = in.readInt();
        }

        @Override
        public int compareTo(IdQuantityPair o) {
            if (this.quantity != o.quantity) {
                return this.quantity < o.quantity ? -1 : 1;
            }
            return (this.id < this.id ? -1 : (this.quantity == o.quantity ? 0 : 1));
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\\|", -1);
            if (parts[0].isEmpty() || parts[0] == null || parts[0].trim().isEmpty()) {
                return;
            }
            if (parts[2].isEmpty() || parts[2] == null || parts[2].trim().isEmpty()) {
                return;
            }
            if (parts[10].isEmpty() || parts[10] == null || parts[10].trim().isEmpty()) {
                return;
            }

            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Query1B inputs");
            int start_date = Integer.parseInt(args[1]);
            int end_date = Integer.parseInt(args[2]);
            int timestamp = Integer.parseInt(parts[0]);

            if (timestamp < start_date || timestamp > end_date) {
                return;
            }

            int item_id = Integer.parseInt(parts[2]);
            int quantity = Integer.parseInt(parts[10]);
            context.write(new IntWritable(item_id), new IntWritable(quantity));
        }
    }

    public static class SumCombiner extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int item_id = key.get();
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(new IntWritable(item_id), new IntWritable(sum));
        }
    }

    public static class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private TreeMap<IdQuantityPair, NullType> map = new TreeMap<IdQuantityPair, NullType>(new Comparator<IdQuantityPair>(){
            @Override
            public int compare(IdQuantityPair o1, IdQuantityPair o2) {
                return o2.compareTo(o1);
            }
        });

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Query1B inputs");
            int k = Integer.parseInt(args[0]);
            int local_sum = 0;
            for (IntWritable val : values) {
                local_sum += val.get();
            }
            map.put(new IdQuantityPair(key.get(), local_sum), null);
            if (map.size() > k) {
                map.pollLastEntry();
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<IdQuantityPair, NullType> entry: map.entrySet()) {
                IntWritable item_id = new IntWritable(entry.getKey().getId());
                IntWritable quantity = new IntWritable(entry.getKey().getQuantity());
                context.write(item_id, quantity);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("Query1B inputs", args);
        Job job = Job.getInstance(conf, "Query 1-b");
        job.setJarByClass(Query1b.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
