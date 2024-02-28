
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Add {

    // Pairing class definition
    public static class Pair implements WritableComparable<Pair> {
        public int i;
        public int j;

        public Pair() {}

        public Pair(int i, int j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(i);
            out.writeInt(j);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            i = in.readInt();
            j = in.readInt();
        }

        @Override
        public int compareTo(Pair p) {
            if (i != p.i) {
                return i < p.i ? -1 : 1;
            } else if (j != p.j) {
                return j < p.j ? -1 : 1;
            } else {
                return 0;
            }
        }

        @Override
        public String toString() {
            return i + "\t" + j;
        }
    }

    // Triple class definition
    public static class Triple implements Writable {
        public int i;
        public int j;
        public double v;

        public Triple() {}

        public Triple(int i, int j, double v) {
            this.i = i;
            this.j = j;
            this.v = v;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(i);
            out.writeInt(j);
            out.writeDouble(v);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            i = in.readInt();
            j = in.readInt();
            v = in.readDouble();
        }

        @Override
        public String toString() {
            return i + "," + j + "," + v;
        }
    }

    // Block class implementation
    public static class Block implements Writable {
        int rows;
        int columns;
        public double[][] data;
        public Pair key;

        public Block() {}

        public Block(int rows, int columns, Pair key) {
            this.rows = rows;
            this.columns = columns;
            this.key = key;
            data = new double[rows][columns];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(rows);
            out.writeInt(columns);
            out.writeInt(key.i);
            out.writeInt(key.j);
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < columns; j++) {
                    out.writeDouble(data[i][j]);
                }
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            rows = in.readInt();
            columns = in.readInt();
            key = new Pair(in.readInt(), in.readInt());
            data = new double[rows][columns];
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < columns; j++) {
                    data[i][j] = in.readDouble();
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();
            s.append("\n");
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < columns; j++) {
                    s.append("\t").append(String.format("%.3f", data[i][j]));
                }
                s.append("\n");
            }
            return s.toString();
        }
    }

    // Mapper for converting sparse matrix to block matrix
    public static class StoB extends Mapper<Object, Text, Pair, Triple> {
        private int rows;
        private int columns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            rows = conf.getInt("rows", 1);
            columns = conf.getInt("columns", 1);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] tokens = value.toString().split(",");
                if (tokens.length != 3) {
                    // Log and skip invalid records
                    context.getCounter("Error", "InvalidRecord").increment(1);
                    return;
                }
                int i = Integer.parseInt(tokens[0]);
                int j = Integer.parseInt(tokens[1]);
                double v = Double.parseDouble(tokens[2]);
                context.write(new Pair(i / rows, j / columns), new Triple(i % rows, j % columns, v));
            } catch (NumberFormatException e) {
                // Log and skip invalid records
                context.getCounter("Error", "InvalidNumberFormat").increment(1);
            }
        }
    }

    // Reducer for converting sparse matrix to block matrix
    public static class StoB_reducer extends Reducer<Pair, Triple, Pair, Block> {
        private int rows;
        private int columns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            rows = conf.getInt("rows", 1);
            columns = conf.getInt("columns", 1);
        }

        public void reduce(Pair key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
            Block block = new Block(rows, columns, key);
            for (Triple triple : values) {
                block.data[triple.i][triple.j] = triple.v;
            }
            context.write(key, block);
        }
    }

    // Mapper for block matrix addition
    public static class Add_Mapper extends Mapper<Pair, Block, Pair, Block> {
        public void map(Pair key, Block value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    // Reducer for block matrix addition
    public static class Add_Reducer extends Reducer<Pair, Block, Pair, Block> {
        public void reduce(Pair key, Iterable<Block> values, Context context) throws IOException, InterruptedException {
            Block resultBlock = null; // Initialize to null
            for (Block block : values) {
                if (resultBlock == null) { // Checking if resultBlock is null
                    resultBlock = new Block(block.rows, block.columns, key); // Initializing resultBlock
                }
                for (int i = 0; i < block.rows; i++) {
                    for (int j = 0; j < block.columns; j++) {
                        resultBlock.data[i][j] += block.data[i][j];
                    }
                }
            }
            if (resultBlock != null) { // Checking if resultBlock is not null
                context.write(key, resultBlock);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            System.err.println("Usage: Add <rows1> <columns1> <input1> <input2> <output1> <output2> <final_output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.setInt("rows", Integer.parseInt(args[0]));
        conf.setInt("columns", Integer.parseInt(args[1]));

        // Creating jobs
        Job job1 = Job.getInstance(conf, "Converting Matrix M to Block Matrix");
        Job job2 = Job.getInstance(conf, "Converting Matrix N to Block Matrix");
        Job job3 = Job.getInstance(conf, "Adding Block Matrices");

        // Configuring first job
        job1.setJarByClass(Add.class);
        job1.setMapperClass(StoB.class);
        job1.setReducerClass(StoB_reducer.class);
        job1.setMapOutputKeyClass(Pair.class);
        job1.setMapOutputValueClass(Triple.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(Block.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[4]));

        // Configure second job
        job2.setJarByClass(Add.class);
        job2.setMapperClass(StoB.class);
        job2.setReducerClass(StoB_reducer.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(Triple.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(Block.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[5]));

        // Configure third job
        job3.setJarByClass(Add.class);
        job3.setMapperClass(Add_Mapper.class);
        job3.setReducerClass(Add_Reducer.class);
        job3.setMapOutputKeyClass(Pair.class);
        job3.setMapOutputValueClass(Block.class);
        job3.setOutputKeyClass(Pair.class);
        job3.setOutputValueClass(Block.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job3, new Path(args[4]), SequenceFileInputFormat.class, Add_Mapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[5]), SequenceFileInputFormat.class, Add_Mapper.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[6]));

        // Executing the jobs
        boolean job1Success = job1.waitForCompletion(true);
        boolean job2Success = job2.waitForCompletion(true);
        boolean job3Success = job3.waitForCompletion(true);

        // Exiting with status indicating success or failure
        System.exit(job1Success && job2Success && job3Success ? 0 : 1);
    }
}