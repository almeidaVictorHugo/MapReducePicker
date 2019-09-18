package advanced.customwritable;

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


public class TransactionMediaPeso {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "mediaPeso");

        //declaracao das classes
        j.setJarByClass(TransactionMediaPeso.class);
        j.setMapperClass(TransactionMediaPeso.MapForAverage.class);
        j.setReducerClass(TransactionMediaPeso.ReduceForAverage.class);
        //j.setCombinerClass(TransactionMediaPeso.ReduceForAverage.class);

        //definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(TransactionWritable.class);

        //definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);



        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
                                                    //entra              //sai
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, TransactionWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] valores = line.split(";");
            if (!valores[6].equals("weight_kg")) {
                if(!valores[6].equals("")){
                    long peso = Long.parseLong(valores[6]);
                    TransactionWritable vlr = new TransactionWritable(peso, 1);
                    String ano = valores[1];
                    con.write(new Text(ano), vlr );
                }
            }

        }
    }

    public static class ReduceForAverage extends Reducer<Text, TransactionWritable, Text, LongWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<TransactionWritable> values, Context con)
                throws IOException, InterruptedException {

           // FloatWritable sum = new FloatWritable(0f);

            long sumO = 0;
            long sumT = 0;
            for(TransactionWritable v: values){
                sumO += v.getOcorrencia();
                sumT += v.getValor();
            }
            long avg = sumT/sumO ;

            //emitir(chave, valor) no formato (palavra, soma das ocorrencias)
            con.write(new Text(word), new LongWritable(avg) );


        }
    }



}

//     0            1       2       3           4           5       6           7               8       9
/*Country_or_area; year; comm_code;commodity  ; flow  ; trade_usd; weight_kg;quantity_name  ;quantity; category
  Afghanistan;    2016;  010410   ;Sheep live ; Export ; 6088     ; 2339     ;Number of items;51      ;01_live_animals*/