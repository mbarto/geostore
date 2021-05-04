package it.geosolutions.geostore.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Before;

public abstract class H2ToPgSQLExporterTest {
    protected H2ToPgSQLExporter exporter;
    @Before
    public void setUp() {
        exporter = new H2ToPgSQLExporter();
    }
    
    protected String getInvalidDbPath() {
        return "WRONGPATH";
    }
    protected String getTestDbPath() throws IOException {
        File tempFile = File.createTempFile("geostore", ".h2.db");
        tempFile.deleteOnExit();
        return tempFile.getAbsolutePath();
    }
    

    protected String getTestDbPathWithoutExtension() throws IOException {
        String path = getTestDbPath();
        return path.substring(0, path.indexOf("."));
    }
    
    
    protected String getTestDb() throws IOException {
        File tempFile = File.createTempFile("geostore", ".h2.db");
        tempFile.delete();
        Files.copy(H2ToPgSQLExporterScriptTest.class.getResourceAsStream("geostore.h2.db"), Paths.get(tempFile.getAbsolutePath()));
        tempFile.deleteOnExit();
        return tempFile.getAbsolutePath();
    }
}
