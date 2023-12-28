package edu.coursera.distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {
    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs     A proxy filesystem to serve files from. See the PCDPFilesystem
     *               class for more detailed documentation of its usage.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs)
            throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        ExecutorService executorService = Executors.newFixedThreadPool(1000);

        while (!socket.isClosed()) {
            Socket s = socket.accept();
            Thread thread = new Thread(() -> {
                    if (socket.isClosed()){
                        return;
                    }
                    try (
                            Socket accepted = s;
                            InputStream inputStream = accepted.getInputStream();
                            OutputStream outputStream = accepted.getOutputStream();
                            PrintWriter writer = new PrintWriter(outputStream);
                    ) {
                        BufferedReader bufferedReader = new BufferedReader(
                                new InputStreamReader(inputStream, StandardCharsets.UTF_8));

                        String line = bufferedReader.readLine();
                        String[] meta = line.split(" ", -1);

                        String file = fs.readFile(new PCDPPath(meta[1]));

                        if (file != null) {
                            String answer = file + "\r\n";
                            writer.write("HTTP/1.0 200 OK\r\n");
                            writer.write("Server: FileServer\r\n");
                            writer.write("\r\n");
                            writer.write((answer));

                        } else {
                            writer.write("HTTP/1.0 404 Not Found\r\n");
                            writer.write("Server: FileServer\r\n");
                        }

                    } catch (IOException e) {
                        System.out.println("O my god!" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
                executorService.submit(thread);

        }
    }
}
