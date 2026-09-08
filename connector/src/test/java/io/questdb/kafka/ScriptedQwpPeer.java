/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.kafka;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Minimal WebSocket/QWP peer for the rebalance regression. */
final class ScriptedQwpPeer implements Closeable {
    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private static final byte STATUS_SCHEMA_MISMATCH = 3;
    private static final int BINARY = 2;
    private static final int CLOSE = 8;
    private static final int PING = 9;
    private static final int PONG = 10;

    private final Thread acceptThread;
    private final List<Client> clients = new CopyOnWriteArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final CountDownLatch firstFrameReceived = new CountDownLatch(1);
    private final AtomicReference<HeldFrame> heldFrame = new AtomicReference<>();
    private final ServerSocket serverSocket;
    private volatile boolean running = true;

    ScriptedQwpPeer() throws IOException {
        serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        acceptThread = new Thread(this::acceptClients, "scripted-qwp-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    void assertHealthy() {
        assertNull(failure.get(), "scripted QWP peer failed");
    }

    void awaitFirstFrame() throws InterruptedException {
        assertTrue(firstFrameReceived.await(30, TimeUnit.SECONDS), "QWP frame was not received");
        assertHealthy();
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    void rejectHeldFrame() throws IOException, InterruptedException {
        HeldFrame frame = heldFrame.get();
        if (frame == null) {
            throw new IllegalStateException("No QWP frame is being held");
        }
        frame.client.sendBinary(errorAck(frame.sequence));
        assertTrue(frame.client.disconnected.await(30, TimeUnit.SECONDS),
                "QWP client did not consume the held rejection");
        assertHealthy();
    }

    @Override
    public void close() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }
        for (Client client : clients) {
            client.close();
        }
        try {
            acceptThread.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static byte[] errorAck(long sequence) {
        return response(STATUS_SCHEMA_MISMATCH, sequence, "bad column");
    }

    private static byte[] response(byte status, long sequence, String message) {
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        ByteBuffer response = ByteBuffer.allocate(1 + Long.BYTES + Short.BYTES + messageBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        response.put(status).putLong(sequence).putShort((short) messageBytes.length).put(messageBytes);
        return response.array();
    }

    private void acceptClients() {
        while (running) {
            try {
                Client client = new Client(serverSocket.accept());
                clients.add(client);
                client.start();
            } catch (IOException e) {
                if (running) {
                    failure.compareAndSet(null, e);
                }
            }
        }
    }

    private synchronized void onBinary(Client client) throws IOException {
        long sequence = client.nextSequence++;
        HeldFrame first = new HeldFrame(client, sequence);
        if (heldFrame.compareAndSet(null, first)) {
            firstFrameReceived.countDown();
            return;
        }
        client.sendBinary(errorAck(sequence));
    }

    private final class Client implements Closeable {
        private final Socket socket;
        private final CountDownLatch disconnected = new CountDownLatch(1);
        private long nextSequence;
        private OutputStream output;
        private Thread thread;

        private Client(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            if (thread != null && Thread.currentThread() != thread) {
                try {
                    thread.join(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void readFrames(InputStream input) throws IOException {
            DataInputStream data = new DataInputStream(input);
            while (running && !socket.isClosed()) {
                int first = data.readUnsignedByte();
                int second = data.readUnsignedByte();
                int opcode = first & 0x0f;
                long length = second & 0x7f;
                if (length == 126) {
                    length = data.readUnsignedShort();
                } else if (length == 127) {
                    length = data.readLong();
                }
                if (length > Integer.MAX_VALUE) {
                    throw new IOException("Oversized WebSocket frame: " + length);
                }
                byte[] mask = (second & 0x80) == 0 ? null : data.readNBytes(4);
                byte[] payload = data.readNBytes((int) length);
                if (payload.length != length || (mask != null && mask.length != 4)) {
                    throw new IOException("Truncated WebSocket frame");
                }
                if (mask != null) {
                    for (int i = 0; i < payload.length; i++) {
                        payload[i] ^= mask[i & 3];
                    }
                }
                if (opcode == BINARY) {
                    onBinary(this);
                } else if (opcode == PING) {
                    writeFrame(PONG, payload);
                } else if (opcode == CLOSE) {
                    writeFrame(CLOSE, payload);
                    return;
                }
            }
        }

        private void start() {
            thread = new Thread(() -> {
                try {
                    InputStream input = socket.getInputStream();
                    output = socket.getOutputStream();
                    handshake(input);
                    readFrames(input);
                } catch (EOFException | SocketException ignored) {
                    // Normal peer-initiated disconnect.
                } catch (IOException e) {
                    if (running && !socket.isClosed()) {
                        failure.compareAndSet(null, e);
                    }
                } finally {
                    disconnected.countDown();
                    clients.remove(this);
                    close();
                }
            }, "scripted-qwp-client-" + socket.getPort());
            thread.setDaemon(true);
            thread.start();
        }

        private synchronized void sendBinary(byte[] payload) throws IOException {
            writeFrame(BINARY, payload);
        }

        private synchronized void writeFrame(int opcode, byte[] payload) throws IOException {
            output.write(0x80 | opcode);
            if (payload.length <= 125) {
                output.write(payload.length);
            } else {
                output.write(126);
                output.write(payload.length >>> 8);
                output.write(payload.length);
            }
            output.write(payload);
            output.flush();
        }

        private void handshake(InputStream input) throws IOException {
            StringBuilder request = new StringBuilder();
            while (!request.toString().endsWith("\r\n\r\n")) {
                int next = input.read();
                if (next < 0 || request.length() >= 8192) {
                    throw new IOException("Invalid WebSocket handshake");
                }
                request.append((char) next);
            }
            String key = null;
            for (String line : request.toString().split("\r\n")) {
                if (line.toLowerCase(Locale.ROOT).startsWith("sec-websocket-key:")) {
                    key = line.substring(line.indexOf(':') + 1).trim();
                }
            }
            if (key == null) {
                throw new IOException("Missing Sec-WebSocket-Key");
            }
            String response = "HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n\r\n";
            output.write(response.getBytes(StandardCharsets.US_ASCII));
            output.flush();
        }
    }

    private static String acceptKey(String key) throws IOException {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] digest = sha1.digest((key + WEBSOCKET_GUID).getBytes(StandardCharsets.US_ASCII));
            return Base64.getEncoder().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    private static final class HeldFrame {
        private final Client client;
        private final long sequence;

        private HeldFrame(Client client, long sequence) {
            this.client = client;
            this.sequence = sequence;
        }
    }
}
