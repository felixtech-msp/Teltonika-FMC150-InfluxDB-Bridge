package io.felixtech.avl8edecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class Utilities {
    private Utilities() {}

    static <T extends Number> void printMap(Map<Integer, String> avlMap, Map<Short, T> varMap) {
        for (var entry : varMap.entrySet()) {
            System.out.println(entry.getKey() + " " + avlMap.get((int) entry.getKey()) + ": " + entry.getValue());
        }
    }

    static void printVarMap(Map<Integer, String> avlMap, Map<Short, byte[]> varMap) {
        for (var entry : varMap.entrySet()) {
            ByteBuffer bb = ByteBuffer.allocate(entry.getValue().length);
            bb.put(entry.getValue());
            System.out.println(entry.getKey() + " " + avlMap.get((int) entry.getKey()) + ": " + bb.getLong());
        }
    }

    static String byteToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            // & 0xFF ensures the byte is treated as unsigned
            hexString.append(String.format("%02X", b));
        }
        return hexString.toString();
    }

    static byte[] hexToBytes(String hex) {
        hex = hex.replaceAll("\\s", "").toUpperCase();

        if (hex.length() % 2 != 0)
            throw new IllegalArgumentException("Odd length");

        byte[] data = new byte[hex.length() / 2];

        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((Character.digit(hex.charAt(i * 2), 16) << 4)
                    + Character.digit(hex.charAt(i * 2 + 1), 16));
        }

        return data;
    }

    static Map<Integer, String> readAvlMap(String file) throws IOException {
        List<String> rows = Files.readAllLines(Path.of(file));
        Map<Integer, String> avlmap = new HashMap<>();

        for (String row : rows) {
            String[] csv = row.split(";");
            int id = Integer.parseInt(csv[0]);
            avlmap.put(id, csv[1]);
        }

        return avlmap;
    }

    static Map<String, String> readImeiMap() throws IOException {
        List<String> rows = Files.readAllLines(Path.of("imei.csv"));
        Map<String, String> imeiMap = new HashMap<>();

        for (String row : rows) {
            String[] csv = row.split(";");
            imeiMap.put(csv[0], csv[1]);
        }

        return imeiMap;
    }
}
