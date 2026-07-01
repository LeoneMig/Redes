import java.io.*;
import java.net.*;
import java.security.*;

public class Receptor {

    // Tempo para aguardar pacotes FIN em retransmissão antes de desligar totalmente
    private static final int ESPERA_FINS_REPETIDOS_MS = 1000;
    
    private final DatagramSocket conexaoUdp;
    private int tetoSequencial;

    // Inicializa o servidor UDP na porta especificada
    Receptor(int portaEscuta) throws SocketException {
        this.conexaoUdp = new DatagramSocket(portaEscuta);
        System.out.printf("receptor operacional -> Aguardando solicitação de handshake na porta %d...%n", portaEscuta);
    }

    // Envia um pacote de reconhecimento (ACK) para o emissor
    private void devolverConfirmacao(int identificadorAck, InetAddress host, int port) throws IOException {
        byte[] pctSerializado = Pacote.converterParaBytes(Pacote.criarAck(identificadorAck));
        conexaoUdp.send(new DatagramPacket(pctSerializado, pctSerializado.length, host, port));
        System.out.printf("receptor ack -> Enviando confirmação do bloco %d%n", identificadorAck);
    }

    // Após terminar, fica escutando um tempo para evitar que o FIN do emissor trave por ACKs perdidos
    private void absorverFinsAtrasados(byte[] bufferReceber, InetAddress host, int port) {
        try {
            conexaoUdp.setSoTimeout(ESPERA_FINS_REPETIDOS_MS);
            while (true) {
                DatagramPacket chegada = new DatagramPacket(bufferReceber, bufferReceber.length);
                conexaoUdp.receive(chegada);
                Pacote pctIsolado = Pacote.converterDeBytes(chegada.getData(), chegada.getLength());
                if (pctIsolado != null && pctIsolado.identificador == Pacote.ID_FIN) {
                    devolverConfirmacao(pctIsolado.sequencia, chegada.getAddress(), chegada.getPort());
                }
            }
        } catch (SocketTimeoutException ignorado) {
            // Fim do tempo limite esperado, sai do loop silenciosamente
        } catch (Exception erroInesperado) {
            erroInesperado.printStackTrace();
        }
    }

    // Loop principal para tratar o handshake e a chegada de dados via Go-Back-N
    void operarServidor() throws Exception {
        byte[] memoriaBuffer = new byte[Pacote.TAM_MAXIMO_PACOTE];

        // --- Etapa 1: Aguarda o primeiro contato (Handshake) ---
        DatagramPacket recepcaoHandshake = new DatagramPacket(memoriaBuffer, memoriaBuffer.length);
        conexaoUdp.receive(recepcaoHandshake);

        Pacote setupPacote = Pacote.converterDeBytes(recepcaoHandshake.getData(), recepcaoHandshake.getLength());
        if (setupPacote == null || setupPacote.contemErro() || setupPacote.identificador != Pacote.ID_HANDSHAKE) {
            System.err.println("receptor erro -> O pacote inicial recebido não corresponde a uma conexão válida.");
            conexaoUdp.close();
            return;
        }

        Pacote.ParametrosConexao metaDados = Pacote.extrairHandshake(setupPacote.payload);
        float configPerda = metaDados.probabilidadeFalha;
        long configTamanho = metaDados.tamanhoTotal;
        int configJanela = metaDados.tamanhoJanela;
        String configSalvar = metaDados.caminhoSalvar;
        this.tetoSequencial = configJanela * 2;

        System.out.println("handshake receptor -> Parâmetros iniciais obtidos com sucesso.");
        System.out.printf("handshake receptor -> Destino local de gravação: %s%n", configSalvar);
        System.out.printf("handshake receptor -> Tamanho total esperado do arquivo: %,d bytes%n", configTamanho);
        System.out.printf("handshake receptor -> Configuração: Janela=%d | Teto Seq=%d | Perda configurada=%.0f%%%n",
                configJanela, tetoSequencial, configPerda * 100);

        InetAddress addrEmissor = recepcaoHandshake.getAddress();
        int portaRemetente = recepcaoHandshake.getPort();

        // Envia resposta do handshake
        devolverConfirmacao(0, addrEmissor, portaRemetente);
        System.out.println("handshake status -> Confirmação de abertura enviada. Preparando recebimento de dados.");

        // --- Etapa 2: Recebimento contínuo de pacotes ---
        int cursorEsperado = 0;
        boolean obteveSucessoAinda = false; // Impede envio de ACKs prematuros se pacotes iniciais falharem
        
        int contagemEscritos = 0;
        int contagemDropados = 0;
        int contagemDescartadosOrdem = 0;
        byte[] hashAssinaturaFin = null;

        // Garante que a pasta destino existe no disco
        File pastaRemota = new File(configSalvar);
        if (pastaRemota.getParentFile() != null) pastaRemota.getParentFile().mkdirs();

        try (BufferedOutputStream streamSaida = new BufferedOutputStream(new FileOutputStream(configSalvar))) {
            while (true) {
                DatagramPacket dadoEntrante = new DatagramPacket(memoriaBuffer, memoriaBuffer.length);
                conexaoUdp.receive(dadoEntrante);

                Pacote pctCorrente = Pacote.converterDeBytes(dadoEntrante.getData(), dadoEntrante.getLength());

                // Caso A: Pacote corrompido (falha no checksum)
                if (pctCorrente == null || pctCorrente.contemErro()) {
                    System.out.println("receptor descarte -> Erro de integridade no pacote (Checksum inválido).");
                    if (obteveSucessoAinda) {
                        devolverConfirmacao((cursorEsperado - 1 + tetoSequencial) % tetoSequencial, addrEmissor, portaRemetente);
                    }
                    continue;
                }

                // Caso B: Fim da transmissão
                if (pctCorrente.identificador == Pacote.ID_FIN && pctCorrente.sequencia == cursorEsperado) {
                    System.out.printf("receptor finalização -> Recebido pacote de encerramento SEQ %d%n", pctCorrente.sequencia);
                    streamSaida.flush(); // Garante que tudo foi escrito no disco
                    hashAssinaturaFin = pctCorrente.payload.length == 16 ? pctCorrente.payload : null;
                    devolverConfirmacao(pctCorrente.sequencia, addrEmissor, portaRemetente);
                    absorverFinsAtrasados(memoriaBuffer, addrEmissor, portaRemetente);
                    break;
                }

                // Caso C: Recebeu pacote de dado correto e no sequencial esperado
                if (pctCorrente.identificador == Pacote.ID_DADO && pctCorrente.sequencia == cursorEsperado) {
                    // Lógica para simular perdas baseadas na probabilidade inserida no Emissor
                    if (Math.random() < configPerda) {
                        contagemDropados++;
                        System.out.printf("receptor descarte -> Forçando perda aleatória do pacote SEQ %d (Total descartado: %d)%n", pctCorrente.sequencia, contagemDropados);
                        // Não envia ACK. Deixa o emissor dar timeout
                    } else {
                        streamSaida.write(pctCorrente.payload);
                        contagemEscritos++;
                        obteveSucessoAinda = true;
                        System.out.printf("receptor processamento -> Gravando bloco SEQ %-5d com %d bytes%n", pctCorrente.sequencia, pctCorrente.payload.length);
                        devolverConfirmacao(cursorEsperado, addrEmissor, portaRemetente);
                        cursorEsperado = (cursorEsperado + 1) % tetoSequencial; // Avança o ponteiro
                    }
                    continue;
                }

                // Caso D: O pacote está perfeito, mas chegou fora de ordem (Característica do Go-Back-N)
                if (pctCorrente.identificador == Pacote.ID_DADO) {
                    contagemDescartadosOrdem++;
                    System.out.printf("receptor descarte -> Desvio de fluxo detectado. Recebido SEQ %d, mas o esperado era %d%n", pctCorrente.sequencia, cursorEsperado);
                    // Reenvia o ACK do último pacote correto para avisar o emissor
                    if (obteveSucessoAinda) {
                        devolverConfirmacao((cursorEsperado - 1 + tetoSequencial) % tetoSequencial, addrEmissor, portaRemetente);
                    }
                }
            }
        }

        conexaoUdp.close();
        gerarSumarioFinal(configSalvar, contagemEscritos, contagemDropados, contagemDescartadosOrdem, configPerda, hashAssinaturaFin);
    }

    // Imprime as estatísticas finais e valida o hash do arquivo recebido com o original
    private void gerarSumarioFinal(String caminhoLocal, int acertos, int falhas, int foraDeOrdem, float chancePerda, byte[] assinaturaEstrangeira) throws Exception {
        int totalEsperadosAtingidos = acertos + falhas;
        double perdaReal = totalEsperadosAtingidos > 0 ? (100.0 * falhas / totalEsperadosAtingidos) : 0;

        System.out.println("\nESTATÍSTICAS FINAIS DO RECEPTOR");
        System.out.printf("Local do arquivo salvo  : %s%n", caminhoLocal);
        System.out.printf("Pacotes aceitos e salvos: %d%n", acertos);
        System.out.printf("Falhas simuladas via UDP: %d%n", falhas);
        System.out.printf("Pacotes fora de ordem   : %d%n", foraDeOrdem);
        System.out.printf("Fluxos em ordem validados: %d%n", totalEsperadosAtingidos);
        System.out.printf("Taxa de erro real medida: %.2f%% (Configuração teórica: %.0f%%)%n", perdaReal, chancePerda * 100);

        // Verificação final do MD5 caso tenha sido recebido no bloco FIN
        if (assinaturaEstrangeira != null) {
            byte[] assinaturaLocal = extrairHashArquivo(new File(caminhoLocal));
            String md5Local = transformarHexadecimal(assinaturaLocal);
            String md5Remoto = transformarHexadecimal(assinaturaEstrangeira);
            
            System.out.printf("Assinatura MD5 local    : %s%n", md5Local);
            if (md5Local.equals(md5Remoto)) {
                System.out.println("Validação de arquivo    : Sucesso total. Integridade garantida.");
            } else {
                System.out.println("Validação de arquivo    : Falha crítica. Divergência nos dados.");
                System.out.printf("Assinatura MD5 esperada : %s%n", md5Remoto);
            }
        }
        System.out.println("FIM DO RESUMO DA SESSÃO\n");
    }

    private static byte[] extrairHashArquivo(File arq) throws Exception {
        MessageDigest verificador = MessageDigest.getInstance("MD5");
        try (FileInputStream leitorStream = new FileInputStream(arq)) {
            byte[] bloco = new byte[8192];
            int qtdLida;
            while ((qtdLida = leitorStream.read(bloco)) != -1) verificador.update(bloco, 0, qtdLida);
        }
        return verificador.digest();
    }

    private static String transformarHexadecimal(byte[] arrBytes) {
        StringBuilder txtHex = new StringBuilder();
        for (byte pedaco : arrBytes) txtHex.append(String.format("%02x", pedaco));
        return txtHex.toString();
    }

    public static void main(String[] argsTerminal) throws Exception {
        int portaOperacao = argsTerminal.length > 0 ? Integer.parseInt(argsTerminal[0]) : 5000;
        Receptor srv = new Receptor(portaOperacao);
        srv.operarServidor();
    }
}