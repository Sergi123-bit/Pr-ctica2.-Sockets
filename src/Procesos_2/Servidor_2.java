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

    private static final Object LOCK = new Object();
    private static final Map<Integer, ClientSession> SESSIONS = new LinkedHashMap<>();
    private static final BlockingQueue<ChatRequest> INBOX = new LinkedBlockingQueue<>();
    private static final BlockingQueue<String> CONSOLE_QUEUE = new LinkedBlockingQueue<>();
    private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

    private static volatile boolean running = true;
    private static volatile boolean firstClientAccepted = false;
    private static int activeClients = 0;
    private static ServerSocket serverSocket = null;

    private static class ClientSession {
        final int id;
        final Socket socket;
        final BufferedReader in;
        final PrintWriter out;
        final String keyword;
        volatile boolean active = true;

        ClientSession(int id, Socket socket, BufferedReader in, PrintWriter out, String keyword) {
            this.id = id;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.keyword = keyword;
        }
    }

    private static class ChatRequest {
        final ClientSession session;
        final String message;

        ChatRequest(ClientSession session, String message) {
            this.session = session;
            this.message = message;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("Uso: Servidor <PORT_SERVIDOR> <MAX_CLIENTES> <PARAULA_CLAU_SERVIDOR>");
            return;
        }

        int port;
        int maxClients;
        String serverKeyword;

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
            startConsoleReader();

            Thread acceptThread = new Thread(() -> acceptLoop(maxClients), "Server-Acceptor");
            acceptThread.setDaemon(true);
            acceptThread.start();

            System.out.println("> Inicializing chat... OK");

            boolean shouldStop = false;

            while (running && !shouldStop) {
                if (firstClientAccepted && getActiveClients() == 0) {
                    System.out.println("> No client connections left. Closing server... OK");
                    shutdownServer();
                    shouldStop = true;
                    continue;
                }

                ChatRequest request = INBOX.poll(500, TimeUnit.MILLISECONDS);
                if (request == null) {
                    continue;
                }

                ClientSession session = request.session;
                if (!session.active) {
                    continue;
                }

                System.out.println("#Rebut del client " + session.id + ": " + request.message);

                String response = waitForConsoleResponse();
                if (response == null) {
                    shouldStop = true;
                    continue;
                }

                System.out.println("#Enviar al client " + session.id + ": " + response);
                sendToSession(session, response);

                if (response.equalsIgnoreCase(serverKeyword)) {
                    System.out.println("> Server keyword detected!");
                    System.out.println("> Closing server... OK");
                    closeAllClients();
                    shutdownServer();
                    shouldStop = true;
                } else {
                    ClientSession toClose = findSessionToClose(response, session.id);
                    if (toClose != null) {
                        System.out.println("> Client keyword detected!");
                        closeSession(toClose);
                    }
                }
            }

        } catch (IOException e) {
            if (running) {
                System.err.println("Error: " + e.getMessage());
            }
        } finally {
            shutdownServer();
        }

        System.out.println("> Bye!");
    }

    private static void acceptLoop(int maxClients) {
        boolean acceptError = false;

        while (running && !acceptError) {
            try {
                Socket socket = serverSocket.accept();

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);

                String clientKeyword = in.readLine();
                if (clientKeyword == null) {
                    closeQuietly(socket);
                    continue;
                }
                clientKeyword = clientKeyword.trim();

                ClientSession session;

                synchronized (LOCK) {
                    if (!running) {
                        closeQuietly(socket);
                        continue;
                    }

                    if (activeClients >= maxClients) {
                        out.println("SERVIDOR_LLENO");
                        closeQuietly(socket);
                        continue;
                    }

                    int id = NEXT_ID.incrementAndGet();
                    session = new ClientSession(id, socket, in, out, clientKeyword);
                    SESSIONS.put(id, session);
                    activeClients++;
                    firstClientAccepted = true;
                }

                System.out.println("> Connection from client " + session.id + " ... OK");

                ClientSession finalSession = session;
                Thread readerThread = new Thread(() -> clientReaderLoop(finalSession),
                        "Client-Reader-" + finalSession.id);
                readerThread.setDaemon(true);
                readerThread.start();

            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting client: " + e.getMessage());
                }
                acceptError = true;
            }
        }
    }

    private static void clientReaderLoop(ClientSession session) {
        String message = null;
        boolean ioError = false;

        try {
            message = (running && session.active) ? session.in.readLine() : null;
        } catch (IOException e) {
            ioError = true;
        }

        while (!ioError && running && session.active && message != null) {
            try {
                INBOX.put(new ChatRequest(session, message));
                message = session.in.readLine();
            } catch (IOException e) {
                ioError = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                message = null;
            }
        }

        closeSession(session);
    }

    private static void startConsoleReader() {
        Thread consoleThread = new Thread(() -> {
            BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
            boolean consoleError = false;

            while (running && !consoleError) {
                try {
                    String line = keyboard.readLine();
                    if (line == null) {
                        consoleError = true;
                        continue;
                    }
                    CONSOLE_QUEUE.put(line);
                } catch (IOException e) {
                    consoleError = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    consoleError = true;
                }
            }
        }, "Console-Reader");

        consoleThread.setDaemon(true);
        consoleThread.start();
    }

    private static String waitForConsoleResponse() {
        String result = null;
        boolean done = false;

        while (running && !done) {
            try {
                String line = CONSOLE_QUEUE.poll(500, TimeUnit.MILLISECONDS);
                if (line != null) {
                    result = line;
                    done = true;
                } else if (firstClientAccepted && getActiveClients() == 0) {
                    done = true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                done = true;
            }
        }

        return result;
    }

    private static boolean sendToSession(ClientSession session, String message) {
        synchronized (session) {
            if (!session.active) {
                return false;
            }
            session.out.println(message);
            session.out.flush();
            if (session.out.checkError()) {
                closeSession(session);
                return false;
            }
            return true;
        }
    }

    private static ClientSession findSessionToClose(String keyword, int currentSessionId) {
        synchronized (LOCK) {
            ClientSession fallback = null;

            for (ClientSession session : SESSIONS.values()) {
                if (!session.active) {
                    continue;
                }

                if (session.keyword.equalsIgnoreCase(keyword)) {
                    if (session.id == currentSessionId) {
                        return session;
                    }
                    if (fallback == null) {
                        fallback = session;
                    }
                }
            }

            return fallback;
        }
    }

    private static void closeSession(ClientSession session) {
        boolean shouldShutdownServer = false;

        synchronized (LOCK) {
            if (!session.active) {
                return;
            }

            session.active = false;
            SESSIONS.remove(session.id);
            activeClients--;

            if (firstClientAccepted && activeClients == 0 && running) {
                shouldShutdownServer = true;
                running = false;
            }
        }

        closeQuietly(session.socket);

        if (shouldShutdownServer) {
            System.out.println("> No client connections left. Closing server... OK");
            closeServerSocketQuietly();
        }
    }

    private static void closeAllClients() {
        running = false;

        List<ClientSession> snapshot;
        synchronized (LOCK) {
            snapshot = new ArrayList<>(SESSIONS.values());
        }

        for (ClientSession session : snapshot) {
            closeSession(session);
        }

        closeServerSocketQuietly();
    }

    private static int getActiveClients() {
        synchronized (LOCK) {
            return activeClients;
        }
    }

    private static void shutdownServer() {
        running = false;
        closeServerSocketQuietly();
    }

    private static void closeServerSocketQuietly() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }}
