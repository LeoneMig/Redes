## Implementação do Protocolo Go-Back-N em Java via UDP

Este repositório contém a implementação do protocolo de transferência confiável Go-Back-N (GBN) operando sobre a camada de transporte UDP, desenvolvido como trabalho final para a disciplina de Redes de Computadores.

O objetivo do projeto é demonstrar, na prática, os conceitos de transferência confiável de dados, utilizando janelas deslizantes, temporizadores e simulação de perda de pacotes.

## Como Compilar

Certifique-se de ter o JDK instalado em sua máquina. O código-fonte está estruturado na pasta src.

```bash
javac -d out src/Pacote.java src/Receptor.java src/Emissor.java
```

## Como executar

O sistema é composto por dois módulos independentes. O Receptor atua como servidor e deve ser iniciado primeiro, pois ele aguarda o pacote inicial de controle (handshake) do Emissor.

Utilize dois terminais diferentes ou máquinas distintas na mesma rede local.

### Terminal 1 — Receptor

```bash
java -cp out Receptor [porta]
```
```bash
java -cp out Receptor
```

porta (opcional) - Padrão: 5000.

Se a porta for omitida, o sistema utilizará a porta 5000 como padrão.

O Receptor exibe `receptor operacional -> Aguardando solicitação de handshake na porta 5000...` e bloqueia até o Emissor conectar.

### Terminal 2 — Emissor

Após o Receptor estar ativo, inicie o processo de transferência pelo Emissor.

Formato dos argumentos:

java -cp out Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> [prob_perda] [porta]

Exemplo :

```bash
java -cp out Emissor ./Dados\hulk.png 127.0.0.1:./DadosRecebidos/hulk_recebido.png 8 0.10 5000
```
```bash
java -cp out Emissor ./Dados\arquivo_1_5MB.txt 127.0.0.1:./DadosRecebidos/arquivo_1_5MB_recebido.txt 8 0.10
```

Lembre-se de colocar o arquivo a ser transferido na pasta "Dados", caso contrário, coloque o caminho do arquivo na parte de origem.

Não se esqueça de renomear as saídas conforme for fazendo a transferência de arquivos distintos.

Ao final da transferência, tanto o Emissor quanto o Receptor exibirão relatórios detalhados no terminal.
