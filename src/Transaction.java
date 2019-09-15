import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Transaction {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "Transaction");

        //declaracao das classes
        j.setJarByClass(Transaction.class);
        j.setMapperClass(Transaction.MapForTransaction.class);
        j.setReducerClass(Transaction.ReduceForTransaction.class);
        j.setCombinerClass(Transaction.ReduceForTransaction.class);

        //definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(TransactionWritable.class);

        //definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);



        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
    //entra              //sai
    public static class MapForTransaction extends Mapper<LongWritable, Text, Text, TransactionWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {




        }
    }

    public static class ReduceForTransaction extends Reducer<Text, TransactionWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<TransactionWritable> values, Context con)
                throws IOException, InterruptedException {



        }
    }


}
