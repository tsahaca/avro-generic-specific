package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

@Slf4j
public class SpecificRecordReaderWriter {

    public static void main(String[] args) throws IOException {
        specificWriter();
        specificRead();
    }

    private static void specificWriter() {
        User.Builder userBuilder = User.newBuilder();
        User user = userBuilder
                .setIsRoot(true)
                .setPassword("PASSWORD")
                .setUserName("BOB")
                .build();
        log.info(user.toString());

        final DatumWriter<User> datumWriter = new SpecificDatumWriter<>(User.class);

        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(user.getSchema(), new File("user-specific.avro"));
            dataFileWriter.append(user);
            log.info("successfully wrote user-specific.avro");
        } catch (IOException ex) {
            log.error("Couldn't write file", ex);
        }
    }

    private static void specificRead() {
        final File file = new File("user-specific.avro");
        final DatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        final DataFileReader<User> dataFileReader;
        try {
            log.info("Reading our specific record");
            dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                Schema schema = dataFileReader.getSchema();
                log.info("Reading the schema from avro file {}", schema.toString());
                User readCustomer = dataFileReader.next();
                log.info(readCustomer.toString());
                log.info("First name: {}", readCustomer.getUserName());
            }
        } catch (IOException ex) {
            log.error("Couldn't write file", ex);
        }
    }

}
