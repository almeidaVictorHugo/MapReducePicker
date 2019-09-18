
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TransactionWritable implements Writable {

    private String valor;
    private int ocorrencia;

    public TransactionWritable(){ }

    public TransactionWritable(String valor, int ocorrencia){
        this.valor = valor;
        this.ocorrencia = ocorrencia;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //tem que ler na mesma ordem de escrita (do metodo write)

        valor = (in.readUTF()) ; //pode dar erro aqui
        ocorrencia = Integer.parseInt(in.readUTF()) ;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(valor));
        out.writeUTF(String.valueOf(ocorrencia));
    }

    public String getValor() {
        return valor;
    }

    public void setValor(String valor) {
        this.valor = valor;
    }

    public int getOcorrencia() {
        return ocorrencia;
    }

    public void setOcorrencia(int ocorrencia) {
        this.ocorrencia = ocorrencia;
    }
}

