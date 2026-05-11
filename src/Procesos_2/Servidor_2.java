package Procesos_2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Servidor_2 {

    // Objeto de sincronización para proteger el acceso concurrente a SESSIONS y activeClients
    private static final Object LOCK = new Object();

    // Mapa de sesiones activas, indexadas por ID de cliente
    private static final Map<Integer, ClientSession> SESSIONS = new LinkedHashMap<>();

    // Cola de mensajes entrantes de los clientes hacia el hilo principal del servidor
    private static final BlockingQueue<ChatRequest> INBOX = new LinkedBlockingQueue<>();

    // Cola de respuestas escritas por el operador del servidor en la consola
    private static final BlockingQueue<String> CONSOLE_QUEUE = new LinkedBlockingQueue<>();

    // Contador atómico para asignar IDs únicos a cada cliente
    private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

    // Flag global que indica si el servidor sigue en ejecución
    private static volatile boolean running = true;

    // Flag que indica si ya se ha aceptado al menos un cliente (para detectar fin de sesiones)
    private static volatile boolean firstClientAccepted = false;

    // Número de clientes actualmente conectados
    private static int activeClients = 0;

    // Socket del servidor, accesible globalmente para poder cerrarlo desde cualquier hilo
    private static ServerSocket serverSocket = null;


    // Clase interna que representa la sesión de un cliente conectado
   
    private static class ClientSession {
        final int id;
        final Socket socket;
        final BufferedReader in;
        final PrintWriter out;
        final String keyword;   // Palabra clave enviada por el cliente en el handshake
        volatile boolean active = true;

        ClientSession(int id, Socket socket, BufferedReader in, PrintWriter out, String keyword) {
            this.id = id;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.keyword = keyword;
        }
    }

  
    // Clase interna que representa un mensaje recibido de un cliente
   
    private static class ChatRequest {
        final ClientSession session;
        final String message;

        ChatRequest(ClientSession session, String message) {
            this.session = session;
            this.message = message;
        }
    }

    // Punto de entrada principal del servidor
   
    public static void main(String[] args) throws InterruptedException {

        // Comprobamos que se han pasado los tres argumentos necesarios
        if (args.length < 3) {
            System.out.println("Uso: Servidor <PORT_SERVIDOR> <MAX_CLIENTES> <PARAULA_CLAU_SERVIDOR>");
            return;
        }

        int port;
        int maxClients;
        String serverKeyword;

        // Parseamos los argumentos y capturamos errores de formato
        try {
            port = Integer.parseInt(args[0]);
            maxClients = Integer.parseInt(args[1]);
            serverKeyword = args[2];
        } catch (NumberFormatException e) {
            System.out.println("Error: el puerto y el número máximo de clientes deben ser numéricos.");
            return;
        }

        System.out.println("> Server chat at port " + port);

        try (ServerSocket ss = new ServerSocket(port)) {
            serverSocket = ss;

            System.out.println("> Inicializing server... OK");

            // Arrancamos el hilo que lee las respuestas del operador por consola
            startConsoleReader();

            // Arrancamos el hilo aceptador de conexiones entrantes
            Thread acceptThread = new Thread(() -> acceptLoop(maxClients), "Server-Acceptor");
            acceptThread.setDaemon(true);
            acceptThread.start();

            System.out.println("> Inicializing chat... OK");

            // Bucle principal del servidor: gestiona los mensajes recibidos de los clientes
            // y espera la respuesta del operador para reenviarla
            boolean serverShouldStop = false;

            while (running && !serverShouldStop) {

                // Si ya hubo algún cliente y no quedan ninguno activo, cerramos el servidor
                if (firstClientAccepted && getActiveClients() == 0) {
                    System.out.println("> No client connections left. Closing server... OK");
                    shutdownServer();
                    serverShouldStop = true; // Sustituye al break original

                } else {
                    // Intentamos obtener el siguiente mensaje de la cola con timeout
                    ChatRequest request = INBOX.poll(500, TimeUnit.MILLISECONDS);

                    // Si no hay mensajes en este intervalo, volvemos a comprobar el estado
                    if (request != null) {

                        ClientSession session = request.session;

                        // Ignoramos mensajes de sesiones que ya se han cerrado
                        if (session.active) {

                            System.out.println("#Rebut del client " + session.id + ": " + request.message);

                            // Esperamos a que el operador escriba una respuesta por consola
                            String response = waitForConsoleResponse();

                            // Si la respuesta es null, el servidor debe cerrarse
                            if (response == null) {
                                serverShouldStop = true; // Sustituye al break original

                            } else {
                                System.out.println("#Enviar al client " + session.id + ": " + response);
                                sendToSession(session, response);

                                // Si la respuesta coincide con la palabra clave del servidor,
                                // cerramos todos los clientes y apagamos el servidor
                                if (response.equalsIgnoreCase(serverKeyword)) {
                                    System.out.println("> Server keyword detected!");
                                    System.out.println("> Closing server... OK");
                                    closeAllClients();
                                    shutdownServer();
                                    serverShouldStop = true; // Sustituye al break original

                                } else {
                                    // Comprobamos si la respuesta coincide con la palabra clave
                                    // de algún cliente para cerrar esa sesión concreta
                                    ClientSession toClose = findSessionToClose(response, session.id);
                                    if (toClose != null) {
                                        System.out.println("> Client keyword detected!");
                                        closeSession(toClose);
                                    }
                                }
                            }
                        }
                    }
                }
            }

        } catch (IOException e) {
            if (running) {
                System.err.println("Error: " + e.getMessage());
            }
        } finally {
            // Aseguramos el cierre limpio del servidor en cualquier caso
            shutdownServer();
        }

        System.out.println("> Bye!");
    }

  
    // Bucle de aceptación de conexiones entrantes
    // Se ejecuta en un hilo dedicado (Server-Acceptor)
  
    private static void acceptLoop(int maxClients) {

        // Seguimos aceptando conexiones mientras el servidor esté activo
        boolean accepting = true;

        while (running && accepting) {
            try {
                Socket socket = serverSocket.accept();

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);

                // Handshake: el cliente envía su palabra clave nada más conectar
                String clientKeyword = in.readLine();

                if (clientKeyword == null) {
                    // El cliente se desconectó antes de completar el handshake
                    closeQuietly(socket);

                } else {
                    clientKeyword = clientKeyword.trim();
                    ClientSession session;

                    synchronized (LOCK) {
                        if (!running) {
                            // El servidor se está cerrando, rechazamos la conexión
                            closeQuietly(socket);
                            session = null;

                        } else if (activeClients >= maxClients) {
                            // Se ha alcanzado el límite de clientes simultáneos
                            out.println("SERVIDOR_LLENO");
                            closeQuietly(socket);
                            session = null;

                        } else {
                            // Registramos la nueva sesión del cliente
                            int id = NEXT_ID.incrementAndGet();
                            session = new ClientSession(id, socket, in, out, clientKeyword);
                            SESSIONS.put(id, session);
                            activeClients++;
                            firstClientAccepted = true;
                        }
                    }

                    // Si la sesión se creó correctamente, arrancamos su hilo lector
                    if (session != null) {
                        System.out.println("> Connection from client " + session.id + " ... OK");

                        ClientSession finalSession = session;
                        Thread readerThread = new Thread(
                                () -> clientReaderLoop(finalSession),
                                "Client-Reader-" + finalSession.id);
                        readerThread.setDaemon(true);
                        readerThread.start();
                    }
                }

            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting client: " + e.getMessage());
                }
                // Si hay un error de IO y el servidor sigue activo, detenemos el bucle aceptador
                accepting = false; // Sustituye al break original
            }
        }
    }

    // Bucle lector de mensajes de un cliente concreto
    // Se ejecuta en un hilo dedicado por cada cliente (Client-Reader-N)

    private static void clientReaderLoop(ClientSession session) {
        try {
            String message;
            // Leemos mensajes del cliente mientras el servidor y la sesión estén activos
            while (running && session.active && (message = session.in.readLine()) != null) {
                // Añadimos el mensaje a la cola para que lo procese el hilo principal
                INBOX.put(new ChatRequest(session, message));
            }
        } catch (IOException e) {
            // Desconexión normal o socket cerrado desde el servidor
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // Al terminar la lectura (por cualquier causa), cerramos la sesión
            closeSession(session);
        }
    }

   
    // Arranca el hilo que lee las respuestas del operador desde la consola
    // y las deposita en CONSOLE_QUEUE para que el hilo principal las consuma
   
    private static void startConsoleReader() {
        Thread consoleThread = new Thread(() -> {
            BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
            try {
                // Leemos líneas de consola mientras el servidor esté activo
                while (running) {
                    String line = keyboard.readLine();
                    if (line == null) {
                        // Entrada estándar cerrada, terminamos el hilo
                        running = false;
                    } else {
                        CONSOLE_QUEUE.put(line);
                    }
                }
            } catch (IOException e) {
                // Entrada de consola cerrada inesperadamente
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "Console-Reader");

        consoleThread.setDaemon(true);
        consoleThread.start();
    }

    
    // Espera a que el operador escriba una respuesta en la consola
    // Retorna null si el servidor debe cerrarse antes de recibir respuesta
 
    private static String waitForConsoleResponse() {
        String result = null;
        boolean waiting = true;

        while (running && waiting) {
            try {
                // Intentamos obtener una línea de consola con timeout de 500ms
                String line = CONSOLE_QUEUE.poll(500, TimeUnit.MILLISECONDS);

                if (line != null) {
                    result = line;
                    waiting = false; // Sustituye al return directo con break implícito

                } else if (firstClientAccepted && getActiveClients() == 0) {
                    // No quedan clientes activos, no tiene sentido seguir esperando
                    waiting = false; // Sustituye al return null con break implícito
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                waiting = false; // Sustituye al return null con break implícito
            }
        }

        return result;
    }

    // Envía un mensaje a una sesión de cliente de forma segura (sincronizada)
    // Retorna false si la sesión ya estaba cerrada o hubo un error de escritura
   
    private static boolean sendToSession(ClientSession session, String message) {
        synchronized (session) {
            if (!session.active) {
                return false;
            }
            session.out.println(message);
            session.out.flush();

            // Comprobamos si hubo algún error al escribir en el socket
            if (session.out.checkError()) {
                closeSession(session);
                return false;
            }
            return true;
        }
    }

    // Busca una sesión cuya palabra clave coincida con la dada
    // Prioriza la sesión actual; si no coincide, devuelve otra que coincida
    
    private static ClientSession findSessionToClose(String keyword, int currentSessionId) {
        synchronized (LOCK) {
            ClientSession fallback = null;

            for (ClientSession session : SESSIONS.values()) {
                if (!session.active) {
                    continue;
                }

                if (session.keyword.equalsIgnoreCase(keyword)) {
                    if (session.id == currentSessionId) {
                        // La sesión actual coincide: la devolvemos directamente
                        return session;
                    }
                    if (fallback == null) {
                        // Guardamos como candidata por si no encontramos la actual
                        fallback = session;
                    }
                }
            }

            return fallback;
        }
    }

    
    // Cierra una sesión individual de cliente de forma segura
    // Si era el último cliente, programa el cierre del servidor
  
    private static void closeSession(ClientSession session) {
        boolean shouldShutdownServer = false;

        synchronized (LOCK) {
            if (!session.active) {
                // La sesión ya fue cerrada anteriormente, evitamos duplicados
                return;
            }

            session.active = false;
            SESSIONS.remove(session.id);
            activeClients--;

            // Si no quedan clientes y el servidor sigue activo, lo cerramos
            if (firstClientAccepted && activeClients == 0 && running) {
                shouldShutdownServer = true;
                running = false;
            }
        }

        // Cerramos el socket del cliente fuera del bloque sincronizado
        closeQuietly(session.socket);

        if (shouldShutdownServer) {
            System.out.println("> No client connections left. Closing server... OK");
            closeServerSocketQuietly();
        }
    }

   
    // Cierra todas las sesiones activas y detiene el servidor
    // Se llama cuando se detecta la palabra clave del servidor

    private static void closeAllClients() {
        running = false;

        // Tomamos una copia de las sesiones para evitar modificar el mapa mientras iteramos
        List<ClientSession> snapshot;
        synchronized (LOCK) {
            snapshot = new ArrayList<>(SESSIONS.values());
        }

        // Cerramos cada sesión individualmente
        for (ClientSession session : snapshot) {
            closeSession(session);
        }

        closeServerSocketQuietly();
    }

   
    // Devuelve el número de clientes activos de forma segura (sincronizada)

    private static int getActiveClients() {
        synchronized (LOCK) {
            return activeClients;
        }
    }

    // Marca el servidor como detenido y cierra el ServerSocket
  
    private static void shutdownServer() {
        running = false;
        closeServerSocketQuietly();
    }

   
    // Cierra el ServerSocket sin lanzar excepciones (silenciosamente)
   
    private static void closeServerSocketQuietly() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // Ignoramos errores al cerrar, ya estamos apagando el servidor
            }
        }
    }

    
    // Cierra un Socket de cliente sin lanzar excepciones (silenciosamente)
    
    private static void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignoramos errores al cerrar el socket del cliente
            }
        }
    }
}
