import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
        Job j = new Job(c, "TransactionCount");

        //declaracao das classes
        j.setJarByClass(Transaction.class);
        j.setMapperClass(MapForTransaction.class);
        j.setReducerClass(ReduceForTransaction.class);
        j.setCombinerClass(ReduceForTransaction.class);

        //definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);



        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
    //entra              //sai
    public static class MapForTransaction extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            String valores[] = line.split(";");
            String nomePais = valores[0].toLowerCase();
            if(nomePais.equals("brazil")){
                
                con.write(new Text("Transacoes envolvendo Brasil "), new IntWritable(1));
            }else{
                con.write(new Text("Transacoes envolvendo Brasil "), new IntWritable(0));
            }

            String ano = valores[1];

            if(!ano.equals("year")){
                con.write(new Text(ano), new IntWritable(1));
            }


        }
    }

    public static class ReduceForTransaction extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v: values){
                sum += v.get();
            }
            con.write(word, new IntWritable(sum));

        }
    }


}