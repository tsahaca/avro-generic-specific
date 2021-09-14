package org.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.reflect.AvroDoc;
import org.apache.avro.reflect.Nullable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserReflected {
    @AvroDoc("Name of the user")
    private String userName;
    @Nullable
    private String password;
    private boolean isRoot;
}
