package musico;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import comum.Mensagem;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class MusicoClient {
    private String nomeMusico;
    private String instrumento;

    private int relogioLocal = 0;
    private Map<String, Integer> relogioVetorial = new ConcurrentHashMap<>();

    // NOVO: Configutações do RabbitMQ
    private final static String EXCHANGE_ORQUESTRA = "canalBroadcastOrquestra"; // O canal de comunicação geral da orquestra
    private final static String QUEUE_MAESTRO = "maestroQueue"; // A "Caixa de Entrada" do Maestro
    private Channel channel;

    private PriorityQueue<Mensagem> filaSolo;
    private Boolean tocandoSolo = false; // Estado do músico em relação ao solo

    // NOVO: Construtor para inicializar a fila de prioridade e definir regras de desempate
    public MusicoClient(String nome, String instrumento) {
        this.nomeMusico = nome;
        this.instrumento = instrumento;
        this.relogioVetorial.put(nome, 0); // O músico entra no próprio relógio vetorial com tempo 0

        filaSolo =  new PriorityQueue<>((musico1, musico2) -> {
            if (musico1.getRelogioLamport() != musico2.getRelogioLamport()) {
                // 1º Critério: Relógio Lamport
                return Integer.compare(musico1.getRelogioLamport(), musico2.getRelogioLamport());
            }
            // 2º Critério: Ordem alfabética do nome
            return musico1.getRemetente().compareTo(musico2.getRemetente());
        });
    }

    // Função para pegar o vetor como texto
    public synchronized String getRelogioVetorial() {
        return this.relogioVetorial.toString();
    }

    // Aumenta os relógios quando envia uma mensagem
    public  synchronized void eventoLocal() {
        this.relogioLocal++;
        if (this.nomeMusico != null) {
            int tempoAtual = this.relogioVetorial.getOrDefault(this.nomeMusico, 0);
            this.relogioVetorial.put(this.nomeMusico, tempoAtual + 1);
        }
    }

    // Ajusta os relógios quando recebe mensagem
    public synchronized void atualizarRelogio(int tempoRecebido, Map<String, Integer> vetorRecebido) {
        this.relogioLocal = Math.max(this.relogioLocal,tempoRecebido);

        if (vetorRecebido != null) {
            for (Map.Entry<String, Integer> entry: vetorRecebido.entrySet()) {
                String processo = entry.getKey();
                int tempoVetor = entry.getValue();

                int tempoAtualVetor = this.relogioVetorial.getOrDefault(processo, 0);
                this.relogioVetorial.put(processo, Math.max(tempoAtualVetor, tempoVetor));
            }
        }
        // NOVO: Chama o evento local para subir o Lamport e o Vetor do Músico em +1
        this.eventoLocal();
    }

    // NOVO: Mudança na lógica de tocar solo (função enviarParaMaestro)
    private void verificarFilaSolo() {
        if (!filaSolo.isEmpty()) {
            Mensagem topo = filaSolo.peek(); // Olha quem é o primeiro da fila

            // Se o primeiro da fila for EU, e eu não estiver tocando ainda...
            if (topo.getRemetente().equals(this.nomeMusico) && !tocandoSolo) {
                tocandoSolo = true;
                this.eventoLocal(); // Sobe os relógios para marcar o início do solo
                System.out.println("\n [Lamport = " + this.relogioLocal + "] *** É A MINHA VEZ! ESTOU TOCANDO MEU SOLO! (" + this.instrumento + ") ***\n");

                // Abre uma thread paralela para simular o tempo do solo (5 segundos)
                new Thread(() -> {
                    try { Thread.sleep(5000); } catch (InterruptedException e) { e.printStackTrace(); }

                    System.out.println("\n [Lamport = " + this.relogioLocal + "] Terminei meu solo. Liberando o palco...");
                    enviarParaMaestro("SOLO_RELEASE", "Terminei");
                    tocandoSolo = false;
                }).start();
            }
        }
    }

    // NOVO: Função de envio padrão
    public void enviarParaMaestro(String tipo, String conteudo){
        this.eventoLocal(); // O relógio sobe obrigatoriamente antes de qualquer envio
        try {
            Mensagem msg = new Mensagem(tipo, conteudo, this.nomeMusico,this.relogioLocal);
            msg.setRelogioVetorial(new HashMap<>(this.relogioVetorial));

            // Joga direto na Queue exclusiva do Maestro (Comunicação Cliente -> Servidor)
            channel.basicPublish("", QUEUE_MAESTRO, null, serializarMensagem(msg));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void menuDoMusico() {
        Scanner sc = new Scanner(System.in);
        System.out.println("\n   [>>>>> Digite 'SOLO' para pedir o palco]");

        while(true) {
            String comando = sc.nextLine();
            if (comando.equalsIgnoreCase("SOLO")) {
                enviarParaMaestro("SOLO_REQUEST", "Quero tocar meu solo!");
                System.out.println("\n[L=" + this.relogioLocal + "] >> Pedido de SOLO enviado. Aguardando...");
            }
        }
    }

    // NOVO: Atualização da função de iniciar o processo para usar RabbitMQ e as novas lógicas
    public void iniciarProcesso(String ipServidor) throws IOException, TimeoutException {
        System.out.println("--- INICIANDO PROCESSO MUSICO (Lamport: " + this.relogioLocal + " | Vetor: " + this.getRelogioVetorial() + ") ---");

        // 1. Conexão com RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ipServidor); // Pode ser "localhost" ou o IP do servidor do Maestro, dependendo do teste
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        // 2. Declara o canal de comunicação geral (Exchange): BROADCAST
        channel.exchangeDeclare(EXCHANGE_ORQUESTRA, "fanout");

        // 3. Fila temporária exclusiva para o Músico receber mensagens do Maestro. Está amarrada ao BROADCAST
        String filaTemporaria = channel.queueDeclare().getQueue();
        channel.queueBind(filaTemporaria, EXCHANGE_ORQUESTRA, "");

        System.out.println("[Lamport = " + this.relogioLocal + " | Vetor = " + this.getRelogioVetorial() + "] MÚSICO CONECTADO: " + this.nomeMusico + " [" + this.instrumento + "]");
        System.out.println("Ouvindo a orquestra...");
        System.out.println("------------------------------------------------");
        // Envia o registro do músico para o Maestro
        enviarParaMaestro("REGISTRO", this.instrumento);

        //4. A escuta assíncrona das mensagens do Maestro (COMANDOS e SOLO)
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                Mensagem msgMaestro = desserializarMensagem(delivery.getBody());
                this.atualizarRelogio(msgMaestro.getRelogioLamport(), msgMaestro.getRelogioVetorial());

                // Ignora o eco das mensagens que ele mesmo mandou pro Broadcast
                if (msgMaestro.getRemetente().equals(this.nomeMusico))
                        return;

                if (msgMaestro.getTipo().equalsIgnoreCase("COMANDO")) {
                    String[] partes = msgMaestro.getConteudo().split(":");
                    String alvo = partes[0];
                    String ordem = partes[1];

                    // NOVO: O FILTRO NO CLIENTE ->  Smart Endpoints e Dumb Pipes (Pontas inteligentes e canais burros)
                    if (alvo.equalsIgnoreCase("TODOS") || alvo.equalsIgnoreCase(this.instrumento))
                        System.out.println("\n[Lamport = " + this.relogioLocal + " | Vetor = " + this.getRelogioVetorial() + "] MAESTRO ORDENOU: " + ordem);
                    else
                        System.out.println("\n[Lamport = " + this.relogioLocal + " | Vetor = " + this.getRelogioVetorial() + "] (Ignorando ordem para " + alvo + ")");
                }
                else if (msgMaestro.getTipo().equalsIgnoreCase("SOLO_REQUEST")) {
                    filaSolo.add(msgMaestro);
                    System.out.println("\n[Lamport = " + this.relogioLocal + "]   >>> " + msgMaestro.getRemetente() + " entrou na fila de solo com Lamport " + msgMaestro.getRelogioLamport() + ". (Tamanho da fila: " + filaSolo.size() + ")");
                    // Verifica se é minha vez de tocar
                    verificarFilaSolo();
                } else if (msgMaestro.getTipo().equalsIgnoreCase("SOLO_RELEASE")) {
                    Mensagem finalMsgMaestro = msgMaestro;
                    filaSolo.removeIf(m -> m.getRemetente().equals(finalMsgMaestro.getRemetente()));
                    System.out.println("\n[Lamport = " + this.relogioLocal + "]   <<< " + msgMaestro.getRemetente() + " liberou o palco. Saiu da fila.");
                    // Passando o canal de comunicação para o próximo da fila, se for o caso
                    verificarFilaSolo();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        // Inicia a escuta assíncrona na fila temporária do Broadcast
        channel.basicConsume(filaTemporaria, true, deliverCallback, consumerTag -> {});
        // Inicia a thread do teclado do Músico para enviar pedidos de solo
        new Thread(() -> menuDoMusico()).start();

//        System.out.println("\n--- FIM DA EXECUCAO ---");
    }

    // --- MÉTODOS AUXILIARES: o RabbitMQ trabalha com bytes, então precisamos converter a mensagem
    private byte[] serializarMensagem(Mensagem obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    private Mensagem desserializarMensagem(byte[] info) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(info);
        ObjectInputStream is = new ObjectInputStream(in);
        return (Mensagem) is.readObject();
    }

    // NOVO: Atualização da Main
    //      Agora apenas cria o objeto e dá o start no processo e agora, as entradas de dados iniciais estão na main também
    public static void main(String[] args) throws IOException, TimeoutException {
        Scanner sc = new Scanner(System.in);
        System.out.println("--- INICIANDO PROCESSO MÚSICO ---");

        // NOVO: Perguntamos o ip do servidor para o cliente. Necessitamos disso para fazer os dois casos de teste.
        // ----- [TESTE 1]: Rodar o MaestroServer e o MusicoClient na mesma máquina, usando "localhost" (Vídeo de 10 minutos).
        // ----- [TESTE 2]: Rodar o MaestroServer e o MusicoClient em máquinas diferentes, usando o IP da máquina do MaestroServer
        //                      (Vídeo de 4 minutos, com o sistema realmente distribuído).
        System.out.println(">> Qual o IP do servidor do Maestro? (Digite 'localhost' se for na mesma máquina): ");
        String ipMaestroServer = sc.nextLine();
        System.out.print(">> Qual seu nome? ");
        String nome = sc.nextLine();
        System.out.print(">> Qual instrumento você toca? ");
        String instrumento = sc.nextLine();

        MusicoClient musico = new MusicoClient(nome, instrumento);
        musico.iniciarProcesso(ipMaestroServer);
    }
}
