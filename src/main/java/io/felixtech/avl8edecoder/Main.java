package io.felixtech.avl8edecoder;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static io.felixtech.avl8edecoder.Utilities.*;

/**
 * Teltonika UDP AVL Data Converter
 */
public final class Main {
    private static boolean DEBUG, TRACCAR;
    private static InfluxDBClient INFLUX;
    private static int PORT;
    private static Map<String, String> IMEI_MAP;
    private static Map<Integer, String> FMC150, FMC650;

    private Main() {}

    static void main() {
        try {
            DEBUG = System.getenv("DEBUG").equals("true");
            TRACCAR = System.getenv("TRACCAR").equals("true");
        } catch (NullPointerException _) {
            DEBUG = false;
            TRACCAR = false;
        }

        try {
            PORT = Integer.parseInt(System.getenv("PORT"));
        } catch (NullPointerException | NumberFormatException _) {
            PORT = 5027;
        }

        try {
            FMC150 = readAvlMap("fmc150.csv");
            FMC650 = readAvlMap("fmc650.csv");
        } catch (IOException ex) {
            System.err.println("Unable to read AVL maps.");
            ex.printStackTrace();
            return;
        }

        try {
            IMEI_MAP = readImeiMap();
        } catch (IOException ex) {
            System.err.println("Unable to read IMEI map.");
            ex.printStackTrace();
            return;
        }

        // connect to InfluxDB 2
        try {
            INFLUX = InfluxDBClientFactory.create(
                    System.getenv("INFLUX_SERVER"),
                    System.getenv("INFLUX_TOKEN").toCharArray(),
                    System.getenv("INFLUX_ORG"),
                    System.getenv("INFLUX_BUCKET"));

            // make sure to properly disconnect when exiting
            Runtime.getRuntime().addShutdownHook(new Thread(INFLUX::close));
        } catch (NullPointerException _) {
            System.err.println("InfluxDB Configuration missing from environment.");
            return;
        }

        // listen to UDP port
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            System.out.println("Listening to " + PORT + "/udp ...");

            while (true) {
                byte[] buffer = new byte[1283];
                DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);

                try {
                    // receive data from telematics module
                    socket.receive(receivePacket);
                    byte[] data = new byte[receivePacket.getLength()];
                    System.arraycopy(buffer, 0, data, 0, receivePacket.getLength());

                    if (TRACCAR) {
                        // forward packet to Traccar (Traccar can't send a AVL ACK, so we have to send it)
                        try (DatagramSocket traccarSocket = new DatagramSocket()) {
                            DatagramPacket traccarPacket = new DatagramPacket(data, data.length, InetAddress.getByName("traccar"), 5027);
                            traccarSocket.send(traccarPacket);
                        }
                    }

                    byte[] responseData = decode(data, receivePacket.getAddress().getHostAddress());

                    // send confirmation back
                    socket.send(new DatagramPacket(responseData, responseData.length, receivePacket.getAddress(), receivePacket.getPort()));

                    if (DEBUG) {
                        System.out.println("----------");
                    }
                } catch (IOException | RuntimeException ex) {
                    System.err.println(ex);
                }
            }
        } catch (SocketException ex) {
            System.err.println(ex);
        }
    }

    private static byte[] decode(byte[] inputData, String sourceIp) {
        // decode UDP datagram as Big Endian Binary Data
        ByteBuffer bb = ByteBuffer.wrap(inputData).order(ByteOrder.BIG_ENDIAN);

        short udpLength = bb.getShort();

        // check if packet length matches UDP header
        if (inputData.length - 2 != udpLength) {
            throw new RuntimeException("Packet size " + inputData.length + " not as advertised " + udpLength);
        }

        short udpPacketId = bb.getShort();

        // check if packet is a Teltonika AVL package (magic number: CAFE)
        if (udpPacketId != -13570) {
            throw new RuntimeException("Not a Teltonika packet: " + String.format("%X", udpPacketId));
        }

        bb.get(); // read useless byte away

        byte packetId = bb.get();
        short imeiLength = bb.getShort();

        byte[] imeiBytes = new byte[imeiLength];
        bb.get(imeiBytes);
        String imei = new String(imeiBytes);

        String codec = String.format("%X", bb.get());

        // check if packet is Codec 8E
        if (!codec.equals("8E")) {
            throw new RuntimeException("Not a Codec 8E packet: " + codec);
        }

        byte numRecords1 = bb.get();
        byte numRecords2 = inputData[inputData.length - 1];

        // check if number of records match
        if (numRecords1 != numRecords2) {
            throw new RuntimeException("Number of Records mismatching: " + numRecords1 + "/" + numRecords2);
        }

        Instant timestamp = Instant.ofEpochMilli(bb.getLong());
        short priority = bb.get(); // 0 = LOW ; 1 = HIGH ; 2 = PANIC
        double longitude = bb.getInt() / 10000000.0; // east – west position TODO calc negative
        double latitude = bb.getInt() / 10000000.0; // north – south position TODO calc negative
        short altitude = bb.getShort(); // meters above sea level
        short angle = bb.getShort(); // degrees from North Pole
        byte satellites = bb.get(); // number of visible satellites
        short speed = bb.getShort(); // speed calculated from satellites

        short eventIoId = bb.getShort();
        short numProperties = bb.getShort();

        short num1byteIo = bb.getShort();
        Map<Short, Byte> map1byteIo = new HashMap<>();

        for (int i = 1; i <= num1byteIo; i++) {
            short id = bb.getShort();
            byte value = bb.get();
            map1byteIo.put(id, value);
        }

        short num2byteIo = bb.getShort();
        Map<Short, Short> map2byteIo = new HashMap<>();

        for (int i = 1; i <= num2byteIo; i++) {
            short id = bb.getShort();
            short value = bb.getShort();
            map2byteIo.put(id, value);
        }
        short num4byteIo = bb.getShort();
        Map<Short, Integer> map4byteIo = new HashMap<>();

        for (int i = 1; i <= num4byteIo; i++) {
            short id = bb.getShort();
            int value = bb.getInt();
            map4byteIo.put(id, value);
        }

        short num8byteIo = bb.getShort();
        Map<Short, Long> map8byteIo = new HashMap<>();

        for (int i = 1; i <= num8byteIo; i++) {
            short id = bb.getShort();
            long value = bb.getLong();
            map8byteIo.put(id, value);
        }

        int realNumProperties = map1byteIo.size() + map2byteIo.size() + map4byteIo.size() + map8byteIo.size();
        if (realNumProperties != numProperties) {
            throw new RuntimeException("Number of properties " + numProperties + " does not match reality: " + realNumProperties);
        }

        short numVarIo = bb.getShort();
        Map<Short, byte[]> mapVarIo = new HashMap<>();

        for (int i = 1; i <= numVarIo; i++) {
            short id = bb.getShort();
            short length = bb.getShort();
            byte[] buffer = new byte[length];
            bb.get(buffer);
            mapVarIo.put(id, buffer);
        }

        if (DEBUG) {
            System.out.println(byteToHex(inputData));
            System.out.println("Source IP Address: " + sourceIp);
            System.out.println("UDP Packet Length: " + udpLength);
            System.out.printf("UDP Packet ID: %X%n", udpPacketId);
            System.out.println("AVL Packet ID: " + packetId);
            System.out.println("IMEI Length: " + imeiLength);
            System.out.println("IMEI: " + imei);
            System.out.println("Codec: " + codec);
            System.out.println("Number of Records 1: " + numRecords1);
            System.out.println("Number of Records 2: " + numRecords2);
            System.out.println("Timestamp: " + timestamp);
            System.out.println("Priority: " + priority);
            System.out.println("Longitude: " + longitude);
            System.out.println("Latitude: " + latitude);
            System.out.println("Altitude: " + altitude);
            System.out.println("Angle: " + angle);
            System.out.println("Satellites: " + satellites);
            System.out.println("Speed: " + speed);
            System.out.println("Event IO ID: " + eventIoId);
            System.out.println("Number of Properties: " + numProperties);
            System.out.println("Number of variable length fields: " + numVarIo);
        } else {
            System.out.printf("Received %s packet %s from %s (%s) with %d records.%n", codec, packetId, sourceIp, imei, numRecords1);
        }

        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("ip", sourceIp);
        dataMap.put("imei", imei);
        dataMap.put("timestamp", timestamp);
        dataMap.put("latitude", latitude);
        dataMap.put("longitude", longitude);
        dataMap.put("altitude", altitude);
        dataMap.put("angle", angle);
        dataMap.put("speed", speed);
        dataMap.put("satellites", satellites);

        String device = IMEI_MAP.get(imei);
        Map<Integer, String> avlmap = switch (device) {
            case "FMC150" -> FMC150;
            case "FMC650" -> FMC650;
            default -> throw new RuntimeException("IMEI is not mapped to a device type");
        };

        for (var entry : map1byteIo.entrySet()) {
            dataMap.put(avlmap.get((int) entry.getKey()).replace(' ', '_'), entry.getValue());
        }

        for (var entry : map2byteIo.entrySet()) {
            dataMap.put(avlmap.get((int) entry.getKey()).replace(' ', '_'), entry.getValue());
        }

        for (var entry : map4byteIo.entrySet()) {
            dataMap.put(avlmap.get((int) entry.getKey()).replace(' ', '_'), entry.getValue());
        }

        for (var entry : map8byteIo.entrySet()) {
            dataMap.put(avlmap.get((int) entry.getKey()).replace(' ', '_'), entry.getValue());
        }

        for (var entry : mapVarIo.entrySet()) {
            dataMap.put(avlmap.get((int) entry.getKey()).replace(' ', '_'), entry.getValue());
        }

        if (DEBUG) {
            printMap(avlmap, map1byteIo);
            printMap(avlmap, map2byteIo);
            printMap(avlmap, map4byteIo);
            printMap(avlmap, map8byteIo);
            printVarMap(avlmap, mapVarIo);
        }

        push(dataMap);

        String returnData = String.format("0005CAFE01%02X%02X", packetId, numRecords1);
        if (DEBUG) System.out.println("Return Data: " + returnData);
        return hexToBytes(returnData);
    }

    private static void push(Map<String, Object> dataMap) {
        final String imei = String.valueOf(dataMap.get("imei"));
        final Instant timestamp = (Instant) dataMap.get("timestamp");

        Point point = Point
                .measurement("teltonika_avl")
                .addTag("imei", imei)
                .time(timestamp, WritePrecision.MS);

        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            String key = entry.getKey();
            if ("imei".equals(key) || "timestamp".equals(key))
                continue;

            Object value = entry.getValue();

            try {
                switch (value) {
                    case Number number -> {
                        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)
                            point.addField(key, number.longValue());
                        else
                            point.addField(key, number.doubleValue());
                    }
                    case byte[] arr -> {
                        long num = 0;
                        // interpret as unsigned big-endian
                        for (byte b : arr) {
                            num = (num << 8) | (b & 0xFFL);
                        }
                        point.addField(key, num);
                    }
                    default -> {
                        point.addField(key, value.toString());
                    }
                }
            } catch (Exception ex) {
                System.err.println("InfluxDB field conversion failed for " + key + ": " + ex.getMessage());
            }
        }

        try {
            if (DEBUG)
                System.out.println(point.toLineProtocol());

            INFLUX.getWriteApiBlocking().writePoint(point);
        } catch (Exception ex) {
            System.err.println("InfluxDB write failed: " + ex.getMessage());
        }
    }
}
