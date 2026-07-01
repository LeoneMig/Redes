import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Pacote {
    
    // Limites de tamanho para os pacotes UDP
    static final int LIMITE_DADOS = 1024;
    static final int TAM_CABECHALHO = 15;
    static final int TAM_MAXIMO_PACOTE = TAM_CABECHALHO + LIMITE_DADOS;

    // Identificadores para definir o tipo de cada pacote trafegado
    static final byte ID_DADO = 0;
    static final byte ID_ACK = 1;
    static final byte ID_HANDSHAKE = 2;
    static final byte ID_FIN = 3;

    // Atributos principais de um pacote na rede
    final byte identificador;
    final int sequencia;
    final int confirmacao;
    final byte[] payload;
    private int checksumArmazenado = -1;

    // Classe auxiliar para armazenar as configurações trocadas no handshake
    static class ParametrosConexao {
        final float probabilidadeFalha;
        final long tamanhoTotal;
        final int tamanhoJanela;
        final String caminhoSalvar;

        ParametrosConexao(float probabilidadeFalha, long tamanhoTotal, int tamanhoJanela, String caminhoSalvar) {
            this.probabilidadeFalha = probabilidadeFalha;
            this.tamanhoTotal = tamanhoTotal;
            this.tamanhoJanela = tamanhoJanela;
            this.caminhoSalvar = caminhoSalvar;
        }
    }

    // Construtor base do pacote
    Pacote(byte identificador, int sequencia, int confirmacao, byte[] payload) {
        this.identificador = identificador;
        this.sequencia = sequencia;
        this.confirmacao = confirmacao;
        this.payload = (payload != null) ? payload : new byte[0];
    }

    // Transforma o objeto Pacote em um array de bytes para envio via rede (UDP)
    static byte[] converterParaBytes(Pacote pacote) {
        ByteBuffer buffer = ByteBuffer.allocate(TAM_CABECHALHO + pacote.payload.length);
        buffer.put(pacote.identificador);
        buffer.putInt(pacote.sequencia);
        buffer.putInt(pacote.confirmacao);
        buffer.putShort((short) pacote.payload.length);
        buffer.putInt(calcularChecksum(pacote.identificador, pacote.sequencia, pacote.confirmacao, pacote.payload));
        buffer.put(pacote.payload);
        return buffer.array();
    }

    // Reconstrói um objeto Pacote a partir dos bytes recebidos da rede
    static Pacote converterDeBytes(byte[] dadosBrutos, int tamanhoTotal) {
        if (tamanhoTotal < TAM_CABECHALHO) return null;
        ByteBuffer buffer = ByteBuffer.wrap(dadosBrutos, 0, tamanhoTotal);
        
        byte id = buffer.get();
        int seq = buffer.getInt();
        int ack = buffer.getInt();
        short tamPayload = buffer.getShort();
        int cs = buffer.getInt();
        
        // Verifica se o tamanho do payload faz sentido
        if (tamPayload < 0 || tamPayload > LIMITE_DADOS || tamanhoTotal < TAM_CABECHALHO + tamPayload) return null;
        
        byte[] corpo = new byte[tamPayload];
        buffer.get(corpo);
        
        Pacote pacoteGerado = new Pacote(id, seq, ack, corpo);
        pacoteGerado.checksumArmazenado = cs;
        return pacoteGerado;
    }

    // Extrai os dados específicos de configuração enviados no primeiro contato
    static ParametrosConexao extrairHandshake(byte[] corpoPacote) {
        ByteBuffer buffer = ByteBuffer.wrap(corpoPacote);
        float taxaPerda = buffer.getFloat();
        long tamArquivo = buffer.getLong();
        int janela = buffer.getInt();
        
        byte[] bytesCaminho = new byte[corpoPacote.length - 16];
        buffer.get(bytesCaminho);
        String destinoSalvar = new String(bytesCaminho, StandardCharsets.UTF_8);
        
        return new ParametrosConexao(taxaPerda, tamArquivo, janela, destinoSalvar);
    }

    // Compara o checksum armazenado com o checksum calculado no momento atual
    boolean contemErro() {
        return checksumArmazenado != -1 && calcularChecksum(identificador, sequencia, confirmacao, payload) != checksumArmazenado;
    }

    // Calcula um valor de verificação somando os bytes do cabeçalho e payload
    private static int calcularChecksum(byte id, int seq, int ack, byte[] dadosBrutos) {
        int somatorio = (id & 0xFF);
        somatorio += ((seq >> 24) & 0xFF) + ((seq >> 16) & 0xFF) + ((seq >> 8) & 0xFF) + (seq & 0xFF);
        somatorio += ((ack >> 24) & 0xFF) + ((ack >> 16) & 0xFF) + ((ack >> 8) & 0xFF) + (ack & 0xFF);
        for (byte pedaco : dadosBrutos) {
            somatorio += (pedaco & 0xFF);
        }
        return somatorio;
    }

    // Métodos fábrica para criar rapidamente cada tipo de pacote
    static Pacote criarDado(int seq, byte[] conteudo) {
        return new Pacote(ID_DADO, seq, 0, conteudo);
    }

    static Pacote criarAck(int confirmacaoNum) {
        return new Pacote(ID_ACK, 0, confirmacaoNum, null);
    }

    static Pacote criarHandshake(float taxaPerda, long tamArq, int tamJanela, String caminhoDestino) {
        byte[] stringBytes = caminhoDestino.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(16 + stringBytes.length);
        buffer.putFloat(taxaPerda);
        buffer.putLong(tamArq);
        buffer.putInt(tamJanela);
        buffer.put(stringBytes);
        return new Pacote(ID_HANDSHAKE, 0, 0, buffer.array());
    }

    static Pacote criarFin(int seqFinal, byte[] hashMd5) {
        return new Pacote(ID_FIN, seqFinal, 0, hashMd5 != null ? hashMd5 : new byte[0]);
    }
}