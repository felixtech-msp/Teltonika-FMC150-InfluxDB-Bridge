package io.felixtech.avl8edecoder;

import java.nio.ByteBuffer;
import java.util.Map;

final class Utilities {
    private Utilities() {}

    static <T extends Number> void printMap(Map<Short, T> map) {
        for (var entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + AVLMap.AVL_NAMES.get((int) entry.getKey()) + ": " + entry.getValue());
        }
    }

    static void printVarMap(Map<Short, byte[]> map) {
        for (var entry : map.entrySet()) {
            ByteBuffer bb = ByteBuffer.allocate(entry.getValue().length);
            bb.put(entry.getValue());
            System.out.println(entry.getKey() + " " + AVLMap.AVL_NAMES.get((int) entry.getKey()) + ": " + bb.getLong());
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
}
