package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TranValorPesoWritable implements Writable {

    private long valor;
    private long ocorrencia;
    private long preco;
    private String mercadoria;

    public TranValorPesoWritable(){ }

    public TranValorPesoWritable(long valor, long ocorrencia, String mercadoria, long preco){
        this.valor = valor;
        this.mercadoria = mercadoria;
        this.ocorrencia = ocorrencia;
        this.preco = preco;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //tem que ler na mesma ordem de escrita (do metodo write)

        valor = Long.parseLong(in.readUTF()) ;
        ocorrencia = Long.parseLong(in.readUTF()) ;
        preco = Long.parseLong(in.readUTF()) ;
        mercadoria = in.readUTF() ;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(valor));
        out.writeUTF(String.valueOf(ocorrencia));
        out.writeUTF(String.valueOf(mercadoria));
    }

    public long getValor() {
        return valor;
    }

    public void setValor(Long valor) {
        this.valor = valor;
    }

    public long getOcorrencia() {
        return ocorrencia;
    }

    public void setOcorrencia(Long ocorrencia) {
        this.ocorrencia = ocorrencia;
    }

    public long getPreco() {
        return preco;
    }

    public void setPreco(long preco) {
        this.preco = preco;
    }

    public String getMercadoria() {
        return mercadoria;
    }

    public void setMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
    }
}

