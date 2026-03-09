package maestro;

import com.rabbitmq.client.*;
import comum.Mensagem;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MaestroServer {
    // RELÓGIOS (A lógica continua igual a da Etapa 4)
    private int relogioMaestro = 0; // Relógio de Lamport do Maestro
    private Map<String, Integer> relogioVetorial = new ConcurrentHashMap<>(); // Relógio Vetorial do Maestro
    // NOVO: Configurações do RABBITMQ
    private final static String EXCHANGE_ORQUESTRA = "canalBroadcastOrquestra"; // O Broadcast
    private final static String QUEUE_MAESTRO = "maestroQueue"; // A Caixa de Entrada do Maestro (Recebe pedidos de solo)
    private Channel channel;
    // NOVO: Modificação da lista de músicos regitrados que agora está simplificada (apenas para o comando LISTAR no console)
    public List<String> listaMusicos = new ArrayList<>();

    // O maestro se coloca no vetor com o tempo 0 ao ser instanciado
    public MaestroServer() { this.relogioVetorial.put("Maestro", 0);}

    // Métodos para gerenciar os relógios do Maestro
    public synchronized int getRelogioMaestro() { return this.relogioMaestro; }
    public synchronized String getRelogioVetorial(){ return this.relogioVetorial.toString(); }

    public synchronized void eventoLocal() {
        this.relogioMaestro++;
        int tempoAtual = this.relogioVetorial.getOrDefault("Maestro", 0);
        this.relogioVetorial.put("Maestro", tempoAtual + 1);
    }
    // A função recebe o Lamport e o Vetor para sincronizar
    public synchronized void atualizarRelogios(int tempoRecebido, Map<String, Integer> vetorRecebido) {
        this.relogioMaestro = Math.max(this.relogioMaestro, tempoRecebido);
        // Regra do Vetor: Fica com o maior tempo de cada processo
        if (vetorRecebido != null) {
            for (Map.Entry<String, Integer> entry : vetorRecebido.entrySet()) {
                String processo = entry.getKey();
                int tempoVetor = entry.getValue();

                int tempoAtualVetor = this.relogioVetorial.getOrDefault(processo, 0);
                this.relogioVetorial.put(processo, Math.max(tempoAtualVetor, tempoVetor));
            }
        }
        // Chama o evento local para subir o Lamport e o Vetor do Maestro em +1
        this.eventoLocal();
    }

    // NOVO: Métodos de Rede (RABBITMQ)
    public void iniciarServidor() throws Exception {
        // 1. Conectando ao servidor RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");   // LOCALHOST, pois o RabbitMQ está rodando na minha máquina
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        // 2, Cria o Broadcast (Exchange do tipo Fanout) para o Maestro falar com todos ao mesmo tempo
        channel.exchangeDeclare(EXCHANGE_ORQUESTRA, "fanout"); // [FANOUT -> ESPALHAR]: Espalha a mensagem para todos na fila

        // 3. Cria a fila (Queue) exclusiva para o Maestro receber mensagens
        channel.queueDeclare(QUEUE_MAESTRO, false, false, false, null);

        System.out.println("------------------------------------------------");
        System.out.println("MAESTRO INICIADO [RabbitMQ] (Lamport: " + this.relogioMaestro + " | Vetor: " + this.getRelogioVetorial() + ")");
        System.out.println("        Aguardando musicos no broker");
        System.out.println("-----------------------------------------------");

        // 4. O substituto do MusicoHandler para o RabbitMQ (Escuta assíncrona)
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                Mensagem msg = desserializarMensagem(delivery.getBody());
                // Sincroniza relógios ao receber a mensagem
                atualizarRelogios(msg.getRelogioLamport(), msg.getRelogioVetorial());

                if (msg.getTipo().equalsIgnoreCase("REGISTRO")) {
                    String infoMusico = msg.getRemetente() + " (" + msg.getConteudo() + ")";
                    listaMusicos.add(infoMusico);
                    System.out.println("[L = " + this.getRelogioMaestro() + " | Vetor = " + this.getRelogioVetorial() + "] NOVO MÚSICO REGISTRADO: " + infoMusico);
                } else if (msg.getTipo().contains("SOLO")) {
                    // O Maestro age como roteador, jogando o pedido no Broadcast para que todos os músicos recebam
                    repassarParaTodos(msg);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        // Ativa a escuta assíncrona na fila do Maestro
        // Argumento 2: "autoAck" = confirma o recebimento da mensagem automaticamente
        channel.basicConsume(QUEUE_MAESTRO, true, deliverCallback, consumerTag -> {});
        // Inicia o menu do Maestro em pararelo para digitar comandos
        new Thread(() -> menuDoMaestro()).start();
    }

    // NOVO: Atualização da função de anunciar ordem para enviar via RabbitMQ
    public void anunciarOrdem(String alvo, String ordem) {
        this.eventoLocal();
        System.out.println(">> [Lamport = " + this.getRelogioMaestro() + " | Vetor = " + this.getRelogioVetorial() + "] Enviando ordem para: " + alvo);

        try {
            Mensagem msg = new Mensagem("COMANDO", alvo + ":" + ordem, "Maestro", this.getRelogioMaestro());
            msg.setRelogioVetorial(new HashMap<>(this.relogioVetorial)); // Passa uma cópia do vetor atual para a mensagem

            // 5. Publica a ordem para que todos os músicos recebam. (Antes era um FOR percorrendo a lista de músicos)
            //  Todos receberão a mensagem, mas cada um irá filtrar pelo ALVO (que está no conteúdo da mensagem) para saber se deve tocar ou não
            channel.basicPublish(EXCHANGE_ORQUESTRA, "", null, serializarMensagem(msg));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // NOVO: (Broadcast) O Maestro age apenas como um megafone para repassar as requisições de solo
    public void repassarParaTodos(Mensagem msg) {
        this.eventoLocal();
        System.out.println(">> [Lamport = " + this.getRelogioMaestro() + " | Vetor = " + this.getRelogioVetorial() + "] [BROADCAST] Repassando " + msg.getTipo() + " de " + msg.getRemetente());
        try {
            // Repassa a mensagem original pelo Broadcast do RabbitMQ
            channel.basicPublish(EXCHANGE_ORQUESTRA, "",null, serializarMensagem(msg));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void menuDoMaestro() {
        Scanner sc = new Scanner(System.in);

        System.out.println("   [Console do Maestro Ativo]");
        System.out.println("   Instruções: Digite o alvo (quem deve tocar) ou LISTAR, aperte ENTER, e depois digite a ordem.");

        while(true) {
            System.out.println("\n>> ALVO (Ex: Violino, Flauta, TODOS) ou LISTAR: ");
            String alvo = sc.nextLine();

            if (alvo.equalsIgnoreCase("LISTAR")) {
                System.out.println("--- " + listaMusicos.size() + " MÚSICOS CONECTADOS (Lamport Maestro: " + this.getRelogioMaestro() + " | Vetor: " + this.getRelogioVetorial() + ") ---");
                for (String m : listaMusicos) {
                    System.out.println(" > " + m);
                }
                continue; // Pula a parte da ORDEM e volta para o ALVO
            }

            System.out.println(">> ORDEM (Ex: Tocar Dó): ");
            String msg = sc.nextLine();
            anunciarOrdem(alvo, msg);
        }
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


    public static void main(String[] args) throws Exception {
        MaestroServer maestro = new MaestroServer();
        maestro.iniciarServidor();
    }
}
