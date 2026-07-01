import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;

public class Emissor {

    // Configurações de tempo e tentativas de envio
    private static final int TEMPO_LIMITE_MS = 500;
    private static final int MAX_TENTATIVAS = 10;
    private static final int NOTIFICAR_A_CADA = 50;

    // Dados de conexão e arquivo
    private final String arqOrigem;
    private final String caminhoRemoto;
    private final InetAddress ipReceptor;
    private final int portaReceptor;
    
    // Parâmetros do protocolo Go-Back-N
    private final int capJanela;
    private final int limiteSequencia;
    private final float probabilidadeErro;
    private final DatagramSocket socketEmissor;

    // Controle de janela deslizante
    private int ponteiroBase = 0;
    private int ponteiroProximo = 0;
    private Pacote[] bufferEnvio;
    
    // Controle de timeout
    private final Timer temporizadorAtivo = new Timer(true);
    private TimerTask tarefaAtual;

    // Métricas para o relatório final
    private int contagemEnviados = 0;
    private int contagemRetransmissoes = 0;
    private int contagemAcks = 0;
    private long instanteInicial;

    // Flag para parar a thread que escuta ACKs ao fim da transferência
    private volatile boolean rodandoAckThread = true;

    // Construtor: inicializa variáveis básicas e o socket UDP
    Emissor(String arqOrigem, String hostEDiretorio, float probabilidadeErro, int capJanela, int porta) throws Exception {
        int divisor = hostEDiretorio.indexOf(':');
        this.ipReceptor = InetAddress.getByName(hostEDiretorio.substring(0, divisor));
        this.caminhoRemoto = hostEDiretorio.substring(divisor + 1);
        this.arqOrigem = arqOrigem;
        this.probabilidadeErro = probabilidadeErro;
        this.capJanela = capJanela;
        this.limiteSequencia = capJanela * 2;
        this.bufferEnvio = new Pacote[capJanela];
        this.portaReceptor = porta;
        this.socketEmissor = new DatagramSocket();
    }

    // Calcula a distância circular entre dois números de sequência
    private int calcularDistanciaCiclica(int frente, int tras) {
        return (frente - tras + limiteSequencia) % limiteSequencia;
    }

    // Inicia ou reinicia o temporizador para a base atual
    private void acionarCronometro() {
        desligarCronometro();
        tarefaAtual = new TimerTask() { public void run() { rotinaDeTimeout(); } };
        temporizadorAtivo.schedule(tarefaAtual, TEMPO_LIMITE_MS);
    }

    // Cancela o temporizador ativo (se houver)
    private void desligarCronometro() {
        if (tarefaAtual != null) {
            tarefaAtual.cancel();
            tarefaAtual = null;
        }
    }

    // Envia fisicamente o pacote pela rede via UDP
    private void dispararUdp(Pacote p) {
        try {
            byte[] raw = Pacote.converterParaBytes(p);
            socketEmissor.send(new DatagramPacket(raw, raw.length, ipReceptor, portaReceptor));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // Imprime o status atual da transferência esporadicamente
    private void informarProgressoAtual() {
        long deltaT = System.currentTimeMillis() - instanteInicial;
        double s = deltaT / 1000.0;
        double vazaoMomento = s > 0 ? (contagemAcks * (double) Pacote.LIMITE_DADOS * 8 / 1e6) / s : 0;
        System.out.printf("emissor progresso -> Enviados: %d | Acks: %d | Retx: %d | Vazão: %.2f Mbps%n",
                contagemEnviados, contagemAcks, contagemRetransmissoes, vazaoMomento);
    }

    // Negocia os parâmetros da transferência antes de enviar os dados
    private void efetuarHandshake(long fileSize) throws Exception {
        Pacote pHandshake = Pacote.criarHandshake(probabilidadeErro, fileSize, capJanela, caminhoRemoto);
        byte[] bits = Pacote.converterParaBytes(pHandshake);
        byte[] recepcao = new byte[Pacote.TAM_CABECHALHO];

        socketEmissor.setSoTimeout(TEMPO_LIMITE_MS);
        for (int tentativa = 1; tentativa <= MAX_TENTATIVAS; tentativa++) {
            socketEmissor.send(new DatagramPacket(bits, bits.length, ipReceptor, portaReceptor));
            System.out.printf("handshake emissor -> Enviando proposta de sincronização (tentativa %d)%n", tentativa);
            try {
                DatagramPacket resposta = new DatagramPacket(recepcao, recepcao.length);
                socketEmissor.receive(resposta);
                Pacote ackRecebido = Pacote.converterDeBytes(resposta.getData(), resposta.getLength());
                if (ackRecebido != null && !ackRecebido.contemErro() && ackRecebido.identificador == Pacote.ID_ACK && ackRecebido.confirmacao == 0) return;
            } catch (SocketTimeoutException ex) {
                System.out.println("handshake aviso -> Tempo limite esgotado, tentando reenviar...");
            }
        }
        throw new RuntimeException("Erro crítico: Conexão inicial recusada após " + MAX_TENTATIVAS + " tentativas.");
    }

    // Bloqueia se a janela estiver cheia, caso contrário envia o pacote e avança o ponteiro
    private synchronized void enviarProtocoloGBN(byte[] carga) throws InterruptedException {
        // Pausa o envio se o limite da janela de transmissão for atingido
        while (calcularDistanciaCiclica(ponteiroProximo, ponteiroBase) >= capJanela) {
            wait();
        }

        Pacote pct = Pacote.criarDado(ponteiroProximo, carga);
        bufferEnvio[ponteiroProximo % capJanela] = pct;
        
        // Se for o primeiro pacote da janela, aciona o temporizador
        if (ponteiroBase == ponteiroProximo) acionarCronometro();
        
        dispararUdp(pct);
        System.out.printf("emissor transmissão -> Enviando SEQ %d com %d bytes%n", ponteiroProximo, carga.length);
        
        contagemEnviados++;
        ponteiroProximo = (ponteiroProximo + 1) % limiteSequencia;
    }

    // Executado em thread separada para capturar as confirmações (ACKs) de forma contínua
    private void ouvirConfirmacoes() {
        byte[] areaMemoria = new byte[Pacote.TAM_CABECHALHO];
        try { socketEmissor.setSoTimeout(100); } catch (SocketException ignored) {}
        
        while (rodandoAckThread) {
            try {
                DatagramPacket pacoteEntrada = new DatagramPacket(areaMemoria, areaMemoria.length);
                socketEmissor.receive(pacoteEntrada);
                Pacote retornoAck = Pacote.converterDeBytes(pacoteEntrada.getData(), pacoteEntrada.getLength());
                
                if (retornoAck == null || retornoAck.contemErro() || retornoAck.identificador != Pacote.ID_ACK) continue;
                tratarChegadaAck(retornoAck.confirmacao);
            } catch (SocketTimeoutException ignored) {
            } catch (Exception erro) {
                if (rodandoAckThread) erro.printStackTrace();
            }
        }
    }

    // Atualiza a base da janela conforme ACKs válidos são recebidos
    private synchronized void tratarChegadaAck(int idConfirmacao) {
        int ocupados = calcularDistanciaCiclica(ponteiroProximo, ponteiroBase);
        int salto = calcularDistanciaCiclica(idConfirmacao, ponteiroBase);
        
        // Ignora ACKs duplicados, velhos ou fora da área da janela
        if (ocupados == 0 || salto >= ocupados) return;

        System.out.printf("emissor confirmação -> Recebido ACK para o pacote %d (Base salta de %d para %d)%n", 
                idConfirmacao, ponteiroBase, (idConfirmacao + 1) % limiteSequencia);
        contagemAcks++;
        ponteiroBase = (idConfirmacao + 1) % limiteSequencia; // Desliza a janela
        
        // Atualiza a situação do temporizador
        if (ponteiroBase == ponteiroProximo) desligarCronometro();
        else acionarCronometro();
        
        notifyAll(); // Libera a thread principal caso ela esteja esperando a janela abrir
    }

    // Retransmite todos os pacotes da janela atual caso ocorra timeout
    private synchronized void rotinaDeTimeout() {
        int pendentes = calcularDistanciaCiclica(ponteiroProximo, ponteiroBase);
        System.out.printf("emissor timeout -> Estouro de tempo detectado. Reenviando %d pacotes pendentes a partir do SEQ %d%n", pendentes, ponteiroBase);
        
        acionarCronometro();
        for (int k = 0; k < pendentes; k++) {
            int seqAtual = (ponteiroBase + k) % limiteSequencia;
            dispararUdp(bufferEnvio[seqAtual % capJanela]);
            System.out.printf("emissor retransmissão -> Reenviando bloco SEQ %d%n", seqAtual);
            contagemEnviados++;
            contagemRetransmissoes++;
        }
    }

    // Sinaliza o fim da transmissão e envia o hash MD5 para o receptor validar
    private void encerrarComFin(byte[] hashArquivo) throws Exception {
        Pacote pctFim = Pacote.criarFin(ponteiroProximo, hashArquivo);
        byte[] rawBytes = Pacote.converterParaBytes(pctFim);
        byte[] espacoAck = new byte[Pacote.TAM_CABECHALHO];

        socketEmissor.setSoTimeout(TEMPO_LIMITE_MS);
        for (int t = 1; t <= MAX_TENTATIVAS; t++) {
            socketEmissor.send(new DatagramPacket(rawBytes, rawBytes.length, ipReceptor, portaReceptor));
            System.out.printf("emissor finalização -> Solicitando encerramento no SEQ %d (Tentativa %d)%n", ponteiroProximo, t);
            try {
                DatagramPacket incoming = new DatagramPacket(espacoAck, espacoAck.length);
                socketEmissor.receive(incoming);
                Pacote respFinal = Pacote.converterDeBytes(incoming.getData(), incoming.getLength());
                
                if (respFinal != null && !respFinal.contemErro() && respFinal.identificador == Pacote.ID_ACK && respFinal.confirmacao == ponteiroProximo) {
                    System.out.println("emissor finalização -> Fim de transmissão validado pelo receptor.");
                    return;
                }
            } catch (SocketTimeoutException ex) {
                System.out.println("emissor aviso -> Sem resposta para o encerramento, tentando novamente...");
            }
        }
        System.out.println("emissor aviso -> O receptor não enviou a confirmação final. Fechando canal por tempo limite.");
    }

    // Orquestra as etapas de leitura do arquivo e envio
    void dispararEnvio() throws Exception {
        File arq = new File(arqOrigem);
        if (!arq.exists()) {
            System.err.println("emissor erro -> Arquivo de origem não encontrado: " + arqOrigem);
            return;
        }
        long tamanhoEmBytes = arq.length();
        byte[] hashCalculado = gerarHashMD5(arq);

        System.out.printf("emissor inicializando -> Carregando arquivo %s com %d bytes%n", arqOrigem, tamanhoEmBytes);
        System.out.printf("emissor inicializando -> Endereço de destino mapeado para %s:%s%n", ipReceptor.getHostAddress(), caminhoRemoto);
        System.out.printf("emissor inicializando -> Janela de envio: %d | Teto sequencial: %d | Erros simulados: %.0f%%%n",
                capJanela, limiteSequencia, probabilidadeErro * 100);

        // Fase 1: Sincronização inicial
        efetuarHandshake(tamanhoEmBytes);
        System.out.println("handshake status -> Parâmetros sincronizados com sucesso. Iniciando envio.");

        instanteInicial = System.currentTimeMillis();
        Thread threadAcks = new Thread(this::ouvirConfirmacoes, "Th-Acks");
        threadAcks.setDaemon(true);
        threadAcks.start();

        // Fase 2: Leitura do disco e envio em partes
        int qtdPacotesPuros = 0;
        try (FileInputStream leitorFluxo = new FileInputStream(arq)) {
            byte[] pedacoLido = new byte[Pacote.LIMITE_DADOS];
            int tamanhoLido;
            while ((tamanhoLido = leitorFluxo.read(pedacoLido)) != -1) {
                enviarProtocoloGBN(Arrays.copyOf(pedacoLido, tamanhoLido));
                qtdPacotesPuros++;
                if (qtdPacotesPuros % NOTIFICAR_A_CADA == 0) informarProgressoAtual();
            }
        }

        // Aguarda todos os pacotes em voo serem confirmados
        synchronized (this) {
            while (ponteiroBase != ponteiroProximo) wait();
            desligarCronometro();
        }
        
        // Para a escuta de ACKs de forma limpa
        rodandoAckThread = false;
        threadAcks.interrupt();
        threadAcks.join(1000);

        // Fase 3: Teardown
        encerrarComFin(hashCalculado);
        socketEmissor.close();

        exibirRelatorioGeral(arqOrigem, tamanhoEmBytes, qtdPacotesPuros, hashCalculado);
    }

    // Exibe os dados finais no terminal
    private void exibirRelatorioGeral(String nomeArq, long tamArq, int pacotesTotais, byte[] assinaturaMd5) {
        long tempoGasto = System.currentTimeMillis() - instanteInicial;
        double segundos = tempoGasto / 1000.0;
        double vazao = segundos > 0 ? (tamArq * 8.0 / 1e6) / segundos : 0;
        
        System.out.println("\nRELATÓRIO DE MÉTRICAS DO EMISSOR");
        System.out.printf("Arquivo processado     : %s (%,d bytes)%n", nomeArq, tamArq);
        System.out.printf("Volume total de blocos : %d%n", pacotesTotais);
        System.out.printf("Total de pacotes salvos: %d (incluindo retransmissões)%n", contagemEnviados);
        System.out.printf("Contagem de timeouts   : %d%n", contagemRetransmissoes);
        System.out.printf("Confirmações recebidas : %d%n", contagemAcks);
        System.out.printf("Tempo de atividade     : %.2f segundos%n", segundos);
        System.out.printf("Vazão média alcançada  : %.2f Mbps%n", vazao);
        System.out.printf("Assinatura MD5 gerada  : %s%n", converterParaHex(assinaturaMd5));
        System.out.println("FIM DOS REGISTROS DE EXECUÇÃO\n");
    }

    private static String converterParaHex(byte[] vetor) {
        StringBuilder construtor = new StringBuilder();
        for (byte b : vetor) construtor.append(String.format("%02x", b));
        return construtor.toString();
    }

    private static byte[] gerarHashMD5(File arquivoAlvo) throws Exception {
        MessageDigest digestor = MessageDigest.getInstance("MD5");
        try (FileInputStream leitor = new FileInputStream(arquivoAlvo)) {
            byte[] pedaco = new byte[8192];
            int lidos;
            while ((lidos = leitor.read(pedaco)) != -1) digestor.update(pedaco, 0, lidos);
        }
        return digestor.digest();
    }

    public static void main(String[] argumentos) throws Exception {
        if (argumentos.length < 3) {
            System.out.println("Uso correto: java Emissor <arquivo> <ip>:<destino> <janela> [probabilidade] [porta]");
            System.out.println("Exemplo: java Emissor foto.png 127.0.0.1:/tmp/foto.png 8 0.10");
            return;
        }
        String fonte = argumentos[0];
        String enderecoAlvo = argumentos[1];
        int valJanela = Integer.parseInt(argumentos[2]);
        float chanceErro = argumentos.length > 3 ? Float.parseFloat(argumentos[3].replace(',', '.')) : 0.10f;
        int ptReceptor = argumentos.length > 4 ? Integer.parseInt(argumentos[4]) : 5000;
        
        Emissor cliente = new Emissor(fonte, enderecoAlvo, chanceErro, valJanela, ptReceptor);
        cliente.dispararEnvio();
    }
}