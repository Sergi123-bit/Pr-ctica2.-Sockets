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

        // Verificamos que se han pasado los tres argumentos obligatorios
        if (args.length < 3) {
            System.out.println("Uso: Cliente <host> <PORT_SERVIDOR> <PARAULA_CLAU_CLIENT>");
            return;
        }

        String host = args[0];
        int port;
        String clientKeyword = args[2];

        // Convertimos el puerto a entero y capturamos posibles errores de formato
        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Error: el puerto debe ser numérico.");
            return;
        }

        System.out.println("> Client chat to port " + port);

        // Abrimos el socket y los streams con try-with-resources
        // para garantizar que se cierran automáticamente al finalizar
        try (Socket socket = new Socket(host, port);
             BufferedReader entrada = new BufferedReader(
                     new InputStreamReader(socket.getInputStream()));
             PrintWriter salida = new PrintWriter(
                     new OutputStreamWriter(socket.getOutputStream()), true);
             Scanner teclado = new Scanner(System.in)) {

            System.out.println("> Inicializing client... OK");

            // Handshake inicial: enviamos nuestra palabra clave al servidor
            // para que pueda identificar y asociar este cliente
            salida.println(clientKeyword);

            System.out.println("> Inicializing chat... OK");

            // Variable de control del bucle principal del chat
            boolean actiu = true;
            String missatge = null;
            String resposta = null;

            // Bucle principal: continúa mientras el chat esté activo,
            // no se detecte la palabra clave ni el servidor cierre la conexión
            while (actiu) {

                // Intentamos leer el mensaje escrito por el usuario por teclado
                try {
                    missatge = teclado.nextLine();
                } catch (Exception e) {
                    // Si falla la lectura (stdin cerrado), detenemos el bucle
                    actiu = false;
                    missatge = null;
                }

                // Solo procesamos si hemos leído un mensaje válido
                if (actiu && missatge != null) {

                    // Enviamos el mensaje al servidor
                    salida.println(missatge);
                    System.out.println("#Enviar al servidor: " + missatge);

                    // Si el mensaje coincide con la palabra clave del cliente,
                    // el cliente cierra voluntariamente la conexión
                    if (missatge.equalsIgnoreCase(clientKeyword)) {
                        System.out.println("> Client keyword detected!");
                        actiu = false; // Sustituye al break original

                    } else {
                        // Esperamos la respuesta del servidor
                        try {
                            resposta = entrada.readLine();
                        } catch (IOException e) {
                            resposta = null;
                        }

                        // Si la respuesta es null, el servidor cerró la conexión
                        if (resposta == null) {
                            System.out.println("> Server closed the chat.");
                            actiu = false; // Sustituye al break original

                        } else {
                            System.out.println("#Rebut del servidor: " + resposta);

                            // Si el servidor responde con la palabra clave del cliente,
                            // indica que el servidor ha ordenado cerrar este chat
                            if (resposta.equalsIgnoreCase(clientKeyword)) {
                                System.out.println("> Client keyword detected!");
                                actiu = false; // Sustituye al break original
                            }
                            // Si no es ninguna palabra clave, el bucle continúa
                        }
                    }
                }
            }

        } catch (IOException e) {
            // Capturamos errores de red o de apertura del socket
            System.err.println("Error: " + e.getMessage());
        }

        // Mensajes de cierre ordenado del cliente
        System.out.println("> Closing chat... OK");
        System.out.println("> Closing client... OK");
        System.out.println("> Bye!");
    }
}
