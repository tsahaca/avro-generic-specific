package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

@Slf4j
public class GenericRecordReaderWriter {
    public static void main(String[] args) throws IOException {
        genericWriter();
        genericReader();
    }
    private static void genericWriter() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new FileInputStream("src/main/resources/avro/user.avsc"));

        GenericRecordBuilder userBuilder = new GenericRecordBuilder(schema);
        userBuilder.set("user_name", "Bob");
        userBuilder.set("password", "mypass");
        userBuilder.set("is_root", false);
        final GenericData.Record user = userBuilder.build();

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(user.getSchema(), new File("user-generic.avro"));
            dataFileWriter.append(user);
            log.info("Written user-generic.avro");
        } catch (IOException ex) {
            log.error("Couldn't write file", ex);
        }
    }

    private static void genericReader() {
        final File file = new File("user-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            customerRead = dataFileReader.next();
            log.info("Successfully read avro file");
            log.info(customerRead.toString());
            log.info("User name: " + customerRead.get("user_name"));
            log.info("Non existent field: " + customerRead.get("non_existing"));
        } catch (IOException ex) {
            log.error("Couldn't write file", ex);
        }
    }
}


