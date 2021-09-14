package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.stream.StreamSupport;

@Slf4j
public class SpecificReflectionReaderWriter {

    public static void main(String[] args) throws IOException {
        reflectionWrite();
        reflectionRead();
    }

    private static void reflectionWrite() {
        try {
            log.info("Writing user-reflected.avro");
            File file = new File("user-reflected.avro");
            Schema schema = ReflectData.get().getSchema(UserReflected.class);
            log.info("schema = " + schema.toString(true));

            DatumWriter<UserReflected> writer = new ReflectDatumWriter<>(UserReflected.class);
            DataFileWriter<UserReflected> out = new DataFileWriter<>(writer)
                    .create(schema, file);

            out.append(new UserReflected("Bob", "any", true));
            out.close();
        } catch (IOException ex) {
            log.error("Couldn't write file", ex);
        }
    }

    private static void reflectionRead() {
        try {
            log.info("Reading user-reflected.avro");
            File file = new File("user-reflected.avro");
            DatumReader<UserReflected> reader = new ReflectDatumReader<>(UserReflected.class);
            DataFileReader<UserReflected> dataFileReader = new DataFileReader<>(file, reader);
            log.info(dataFileReader.getSchema().toString());
            StreamSupport.stream(dataFileReader.spliterator(), false)
                    .forEach(userReflected -> log.info(userReflected.toString()));
            dataFileReader.close();
        } catch (IOException ex) {
            log.error("Couldn't write file", ex);
        }
    }
}
