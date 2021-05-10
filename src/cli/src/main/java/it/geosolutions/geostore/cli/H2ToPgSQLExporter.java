/*
 * ====================================================================
 *
 * Copyright (C) 2021 GeoSolutions S.A.S.
 * http://www.geo-solutions.it
 *
 * GPLv3 + Classpath exception
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. 
 *
 * ====================================================================
 *
 * This software consists of voluntary contributions made by developers
 * of GeoSolutions.  For more information on GeoSolutions, please see
 * <http://www.geo-solutions.it/>.
 *
 */
package it.geosolutions.geostore.cli;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.h2.tools.Script;
import io.vavr.control.Either;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Utility to export the GeoStore database from the H2 file to a PostgreSQL database.
 * Produces an SQL script to insert all the data in an exising PgSQL database, with the GeoStore schema.
 * 
 */
public class H2ToPgSQLExporter implements Runnable {
    
    enum OutputType {
        STDOUT,
        FILE
    }
    
    static class Output {
        public OutputType type;
        public Optional<String> path = Optional.empty();
        
        private Output(OutputType type) {
            this.type = type;
        }
        
        private Output(String path) {
            this.type = OutputType.FILE;
            this.path = Optional.of(path);
        }
        
        static Output stdout() {
            return new Output(OutputType.STDOUT);
        }
        
        static Output path(String path) {
            return new Output(path);
        }
    }

    @Parameters(paramLabel = "H2FILE", description = "path to the H2 database file to migrate", arity="1")
    String inputPath;
    
    @Option(names = {"-o", "--output"}, description = "path to output SQL file", arity="0..1")
    String outputPath;
    
    @Option(names = {"-u", "--user"}, description = "H2 database username", defaultValue = "geostore")
    String username;

    @Option(names = {"-p", "--password"}, description = "H2 database password", defaultValue = "geostore")
    String password;

    List<String> orderedTables = Arrays.asList(new String[] {"GS_CATEGORY", "GS_RESOURCE", "GS_ATTRIBUTE", "GS_USER", "GS_USER_ATTRIBUTE", "GS_USERGROUP",
            "GS_USERGROUP_MEMBERS", "GS_SECURITY", "GS_STORED_DATA"});
    
    Pattern searchInserts = Pattern.compile("INSERT INTO PUBLIC\\.([A-Z_]+)\\([^)]+\\) VALUES\\s*(.*?);(\n|\r\n)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    Pattern searchDecode = Pattern.compile("STRINGDECODE\\(('.*')\\)", Pattern.CASE_INSENSITIVE);
    
    @Override
    public void run() {
        String message = validateInputFile()
            .flatMap(input -> extractAndScript(input))
            .flatMap(script -> writeOutput(script))
            .fold(e -> e, s -> "Export done!");
       
        System.out.println(message);
    }

    
    Either<String, Boolean> writeOutput(String sql) {
        return validateOutputFile()
             .flatMap(output -> 
                     output.type == OutputType.STDOUT ?
                             writeToStdOut(sql) : 
                             writeToFile(output.path.get(), sql)
                     );
    }


    private Either<String, Boolean> writeToStdOut(String sql) {
        System.out.println(sql);
        return Either.right(true);
    }
    
    private Either<String, Boolean> writeToFile(String path, String sql) {
        try {
            Files.write(Paths.get(path), sql.getBytes(StandardCharsets.UTF_8));
            return Either.right(true);
        } catch (IOException e) {
            return Either.left("Error writing to output file: " + e.getMessage());
        }
    }
    
    Either<String, String> extractAndScript(File input) {
        return exportH2AsScript(input)
                .map(script -> filterInserts(script))
                .map(sortedInserts -> sortedInserts.stream().map(i -> normalizeInsert(i)).collect(Collectors.toList()))
                .map(normalized -> createSql(normalized, getLastUsedId(normalized)));
        
    }


    private Comparator<String> buildTablesComparator() {
        return new Comparator<String>() {
            @Override
            public int compare(String t1, String t2) {
                return Integer.compare(orderedTables.indexOf(t1), orderedTables.indexOf(t2));
            }
        };
    }


    int getLastUsedId(List<String> normalized) {
        return normalized.stream().map(i -> getId(i)).reduce(0, (max, id) -> Math.max(max, id));
    }


    private int getId(String i) {
        return Arrays.asList(i.split("\n")).stream()
                .filter(s -> s.startsWith("("))
                .map(s -> Integer.parseInt(s.substring(1).split(",")[0]))
                .reduce(0, (max, id) -> Math.max(max, id));
    }


    String createSql(List<String> inserts, int lastId) {
        return StringUtils.join(inserts, "\n") + "ALTER SEQUENCE GEOSTORE.HIBERNATE_SEQUENCE RESTART WITH " + lastId + ";\n";
    }


    private List<String> flat(Map<String, List<String>> insertsByTable) {
        return insertsByTable.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }


    List<String> filterInserts(String script) {
        Map<String, List<String>> insertsByTable = new TreeMap<String, List<String>>(buildTablesComparator());
        Matcher m = searchInserts.matcher(script);
        while (m.find()) {
            String tableName = m.group(1);
            String insert = m.group(0);
            if (insertsByTable.containsKey(tableName)) {
                insertsByTable.get(tableName).add(insert);
            } else {
                insertsByTable.put(tableName, new ArrayList<String>(Arrays.asList(insert)));
            }
        }
        return flat(insertsByTable);
    }


    Either<String, String> exportH2AsScript(File input) {
        try(ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            try {
                Script.execute("jdbc:h2:" + input.getAbsolutePath(), username, password, os);
                String script = new String(os.toByteArray(), StandardCharsets.UTF_8);
                return Either.right(script);
            } catch(SQLException e) {
                return Either.left("Error extracting data from the H2 database: " + e.getMessage());
            }
        } catch (IOException e1) {
            return Either.left("Error extracting data from the H2 database: " + e1.getMessage());
        }
    }
    

    String normalizeInsert(String insert) {
        
        String result = insert.replace("INSERT INTO PUBLIC.", "INSERT INTO GEOSTORE.");
        Matcher m = searchDecode.matcher(result);
        while (m.find()) {
            String value = m.group(1);
            result = result.replace(m.group(0), value.replaceAll("\\\\\"", Matcher.quoteReplacement("\"")));
        }
        return result;
    }


    Either<String, File> validateInputFile() {
        return exists(inputPath) ?
                Either.right(removeExtension(inputPath)) :
                Either.left("H2 file not found: "+ inputPath);
    }


    private boolean exists(String path) {
        return new File(addDbExtension(path)).exists();
    }


    private String addDbExtension(String path) {
        if(path.toLowerCase().endsWith(".h2.db"))
            return path;
        if(path.endsWith(".h2"))
            return path + ".db";
        return path + ".h2.db";
    }
    
    private String addScriptExtension(String path) {
        if(path.toLowerCase().endsWith(".sql"))
            return path;
        return path + ".sql";
    }
    
    private File removeExtension(String path) {
        if(path.indexOf(".") != -1)
            return new File(path.substring(0, path.indexOf(".")));
        return new File(path);
    }


    public Either<String, Output> validateOutputFile() {
        if (outputPath == null)
            return Either.right(Output.stdout());
        File folder = new File(outputPath).getAbsoluteFile().getParentFile();
        if (folder.exists() && folder.isDirectory()) {
            return Either.right(Output.path(addScriptExtension(outputPath)));
        }
        return Either.left("The destination file folder does not exist or is not a folder");
    }

}

