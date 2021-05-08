package com.manolo.firebase2bq;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;

public class TestBin {
    private static final Logger log = Logger.getLogger( Loader.class.getName() );

    public static void main(String args[]){
        DataInputStream din = null;
        PrintWriter printWriter = null;
        int count = 0;
        try {
            String fileInput = args[0];
            String fileOutput = args[1];

            /*byte[] inputArr = Files.readAllBytes(Paths.get(fileInput));
            byte[] outputArr = decompress(inputArr) ;
            Path pathOut = Paths.get(fileOutput);
            Files.write(pathOut, outputArr);*/
            
            //decompressGzip(Paths.get(fileInput), Paths.get(fileOutput));

            File fIn = new File(fileInput);
            long numBytes = fIn.length();
            System.out.println("numBytes: " + numBytes);
            din = new DataInputStream(new FileInputStream(fIn));
            printWriter = new PrintWriter(new File(fileOutput));

            float f = 0f;
            while(4 * count < numBytes){
                f = din.readFloat();
                count++;
                printWriter.println(f);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally{
            try {din.close(); } catch (Exception e) { }
            try {printWriter.close(); } catch (Exception e) { }
        }
        System.out.println("Readed: " + count  + " items");        
    }

    public static byte[] compress(byte[] data) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

        deflater.finish();
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();

        log.log(Level.INFO, "Original: " + data.length / 1024 + " Kb");
        log.log(Level.INFO, "Compressed: " + output.length / 1024 + " Kb");
        return output;
    }

    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();

        log.log(Level.INFO, "Original: " + data.length);
        log.log(Level.INFO, "Compressed: " + output.length);
        return output;
    }

    public static void decompressGzip(Path source, Path target) throws IOException {

        try (GZIPInputStream gis = new GZIPInputStream(
                                      new FileInputStream(source.toFile()));
             FileOutputStream fos = new FileOutputStream(target.toFile())) {

            // copy GZIPInputStream to FileOutputStream
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }

        }

    }
}
