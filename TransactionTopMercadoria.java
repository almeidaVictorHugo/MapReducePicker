package advanced.customwritable;

import advanced.customwritable.utils.MiscUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.HashMap;
import java.util.Map;


public class TransactionTopMercadoria {
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
        j.setJarByClass(TransactionTopMercadoria.class);
        j.setMapperClass(MapForTransaction.class);
        j.setReducerClass(ReduceForTransaction.class);
        j.setCombinerClass(CombinerForTransaction.class);

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
            //separando valores em uma array
            String valores[] = line.split(";");

            //valores da lista
            String nomePais = valores[0].toLowerCase();
            String ano = valores[1];
            String mercadoria = valores[3];


            if(nomePais.equals("brazil")){
                if(ano.equals("2016")){
                    con.write(new Text(mercadoria), new IntWritable(1));
                }
           }
        }
    }

    public static class ReduceForTransaction extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Text, IntWritable> countMap = new HashMap<>();
        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v: values){
                sum += v.get();
            }
            // ao inves de mandar direto para o output, o count map eh preenchido com os resultados
            countMap.put(new Text(word), new IntWritable(sum));

        }

        //para que seja retornado algo ao output apos todos os dados coletados
        protected void cleanup(Context context) throws IOException, InterruptedException {

            //cria um map ordenado com os elementos pelo valor
            Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                //seleciona apenas os 5 primeiros elementos da lista
                if (counter++ == 5) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    public static class CombinerForTransaction extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


}