package Query1;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
* Command line structure to run the code
* bin/hadoop jar your_package.jar main N start_date end_date input_file output_directory
*/

public class Query1c{
    // Create a pair class to address the key conflict in the TreeMap
    public static class IntDoublePair implements WritableComparable<IntDoublePair> {
        private int date;
        private Double netPay;

        public IntDoublePair() {
            super();
        }

        public IntDoublePair(int date, Double netPay) {
            super();
            this.date = date;
            this.netPay = netPay;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(date);
            out.writeDouble(netPay);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            date = in.readInt();
            netPay = in.readDouble();
        }

        @Override
        public int compareTo(IntDoublePair o) {
            if (this.netPay != o.netPay) {
                return this.netPay < o.netPay ? -1 : 1;
            }
            return (this.date < o.date ? -1 : (this.netPay == o.netPay ? 0 : 1));
        }

        public void set(int date, Double netPay) {
            this.date = date;
            this.netPay = netPay;
        }

        public int getDate() {
            return date;
        }

        public void setDate(int date) {
            this.date = date;
        }

        public Double getNetPay() {
            return netPay;
        }

        public void setNetPay(Double netPay) {
            this.netPay = netPay;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + date;
            result = prime * result + (int)Math.round(netPay);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            IntDoublePair other = (IntDoublePair) obj;
            if (date != other.date)
                return false;
            if (netPay != other.netPay)
                return false;
            return true;
        }

    }

    public static class HighestValueNetPayMapper extends Mapper<Object, Text, IntWritable, IntDoublePair>{
        @Override
        protected void map(Object key1, Text value1, Context context) throws IOException, InterruptedException {
            // data example: 2451813|65495|3617|67006|591617|3428|24839|10|161|1|79|11.41|18.71|2.80|99.54|221.20|901.39|1478.09|6.08|99.54|121.66|127.74|-779.73|
            String data = value1.toString();
            // split the data
            String[] data_array = data.split("\\|", -1);

            int ss_sold_date;  // in the schema, the index is 0.
            Double ss_net_paid_inc_tax;  // in the schema, the index is 21.

            // Get the input timestamps
            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Query1C inputs");
            int start_date = Integer.parseInt(args[1]);
            int end_date = Integer.parseInt(args[2]);
            // Check whether the timestamp part is valid.
            if (data_array[0].isEmpty() || data_array[0] == null || data_array[0].trim().isEmpty()) {
                return;
            }
            else{
                // Get the current data's timestamp
                int temp_timestamp = Integer.parseInt(data_array[0]);
                if (temp_timestamp < start_date || temp_timestamp > end_date) {
                    return;
                }
                ss_sold_date = temp_timestamp;
            }
            // Check whether the ss_net_paid_inc_tax part is valid.
            if (data_array[21].isEmpty() || data_array[21] == null || data_array[21].trim().isEmpty()) {
                return;
            }
            else {
                ss_net_paid_inc_tax = Double.parseDouble(data_array[21]);
            }
            // Mapper's output
            IntDoublePair dateNetPayPair = new IntDoublePair(ss_sold_date, ss_net_paid_inc_tax);
            context.write(new IntWritable(ss_sold_date), dateNetPayPair);

        }
    }

    public static class CustomCombiner extends Reducer<IntWritable, IntDoublePair,IntWritable, IntDoublePair> {
        public void reduce(IntWritable key, Iterable<IntDoublePair> values, Context context) throws IOException, InterruptedException {
            Double localSum = 0D;
            int date = 0;
            for (IntDoublePair val : values) {
                localSum += val.getNetPay();
                date = val.getDate();
            }
            IntDoublePair outValue = new IntDoublePair(date, localSum);
            context.write(key, outValue);
        }
    }

    public static class HighestValueNetPayReducer extends Reducer<IntWritable, IntDoublePair, Text, DoubleWritable> {
        private TreeMap<IntDoublePair, Integer> treeMap = new TreeMap<IntDoublePair, Integer>(
                new Comparator<IntDoublePair>() {
                    @Override
                    // In order to implement the descending order
                    public int compare(IntDoublePair x, IntDoublePair y){
                        return y.compareTo(x);
                    }
                });

        @Override
        public void reduce(IntWritable key, Iterable<IntDoublePair> values, Context context)
                throws IOException, InterruptedException{
            int soldDate = key.get();
            Double local_sum = 0D;
            for (IntDoublePair val: values) {
                local_sum += val.getNetPay();
            }

            Configuration cfg = context.getConfiguration();
            String args[] = cfg.getStrings("Query1C inputs");
            int top_k = Integer.parseInt(args[0]);

            IntDoublePair dateNetPayPair = new IntDoublePair(soldDate, local_sum);
            treeMap.put(dateNetPayPair, soldDate);

            if (treeMap.size() > top_k) {
                treeMap.pollLastEntry();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<IntDoublePair, Integer> entry: treeMap.entrySet()) {
                IntDoublePair dateNetPayPair = entry.getKey();
                Double ss_net_inc_tax = dateNetPayPair.getNetPay();
                String ss_sold_date = Integer.toString(entry.getValue());
                context.write(new Text(ss_sold_date), new DoubleWritable(ss_net_inc_tax));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        // Get the parameters from the command line
        conf.setStrings("Query1C inputs",args);
        Job job = Job.getInstance(conf, "Query1-c");
        job.setJarByClass(Query1c.class);
        // Mapper class
        job.setMapperClass(HighestValueNetPayMapper.class);
        // Combiner class
        job.setCombinerClass(CustomCombiner.class);

        // Reducer class
        job.setReducerClass(HighestValueNetPayReducer.class);

        // Mapper output class
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntDoublePair.class);

        // Reducer output class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Input and Output file path
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
