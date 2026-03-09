package comum;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Mensagem implements Serializable {
    private static final long serialVersionUID = 1L;
    private String tipo; // "COMANDO", "REGISTRO"
    private String conteudo; // "Toque Dó", "Violino"
    private String remetente; // "Maestro", "Musico1"

    // NOVO: O Relógio de Lamport
    private int relogioLamport;
    // NOVO: Os Relógios Vetoriais
    private Map<String, Integer> relogioVetorial; // Chave: Nome do músico, Valor: Tempo do relógio


    // NOVO: Construtor atualizado para incluir o relógio de Lamport e inicializar o vetor
    public Mensagem(String tipo, String conteudo, String remetente, int relogioLamport) {
        this.tipo = tipo;
        this.conteudo = conteudo;
        this.remetente = remetente;
        this.relogioLamport = relogioLamport;
        this.relogioVetorial = new HashMap<>();
    }

    @Override
    public String toString() {
        // Atualuzação: Inclusão dos dois relógios na representação da mensagem
        return "[Lamport: " + relogioLamport + " | Vetor: " + relogioVetorial + "] " + remetente + " (" + tipo + "): " + conteudo;
    }

    public String getTipo() { return tipo; }
    public String getConteudo() { return conteudo; }
    public String getRemetente() { return remetente; }
    public int getRelogioLamport() { return relogioLamport; }
    public Map<String, Integer> getRelogioVetorial() { return relogioVetorial; }
    public void setRelogioVetorial(Map<String, Integer> relogioVetorial) {
        this.relogioVetorial = relogioVetorial;
    }
}
