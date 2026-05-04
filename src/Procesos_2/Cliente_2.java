package Procesos_2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class Cliente_2 {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: Cliente <host> <PORT_SERVIDOR> <PARAULA_CLAU_CLIENT>");
            return;
        }

        String host = args[0];
        int port;
        String clientKeyword = args[2];

        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Error: el puerto debe ser numérico.");
            return;
        }

        System.out.println("> Client chat to port " + port);

        try (Socket socket = new Socket(host, port);
             BufferedReader entrada = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter salida = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             Scanner teclado = new Scanner(System.in)) {

            System.out.println("> Inicializing client... OK");

            // Handshake: el servidor guarda esta palabra clave para identificar este chat.
            salida.println(clientKeyword);

            System.out.println("> Inicializing chat... OK");

            boolean actiu = true;

            while (actiu) {
                String missatge;
                try {
                    missatge = teclado.nextLine();
                } catch (Exception e) {
                    break;
                }

                salida.println(missatge);
                System.out.println("#Enviar al servidor: " + missatge);

                if (missatge.equalsIgnoreCase(clientKeyword)) {
                    System.out.println("> Client keyword detected!");
                    break;
                }

                String resposta = entrada.readLine();
                if (resposta == null) {
                    System.out.println("> Server closed the chat.");
                    break;
                }

                System.out.println("#Rebut del servidor: " + resposta);

                if (resposta.equalsIgnoreCase(clientKeyword)) {
                    System.out.println("> Client keyword detected!");
                    break;
                }
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("> Closing chat... OK");
        System.out.println("> Closing client... OK");
        System.out.println("> Bye!");
    }
}