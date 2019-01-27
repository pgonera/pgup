package pl.progon.pgup;

import org.apache.commons.io.FileUtils;

import java.io.DataInput;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.System.exit;

public class DbUpApplication {
    public static final String DIR = "dir";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String DBNAME = "db";
    public static final String USER = "user";
    public static final String PASS = "pass";
    private String[] attrs;
    private Map<String, String> env;
    private List<String> filenames;
    private List<String> executedFiles;

    public DbUpApplication(String[] attrs) {
        this.attrs = attrs;
        this.env = new HashMap<String, String>();
        this.executedFiles = new ArrayList<>();
    }

    public void run() {
        try {
            parseAttrs();
            enumerateFiles();
            fetchExecutedFiles();
            processFiles();
        } catch (Exception ex) {
            ex.printStackTrace();
            exit(0);
        }
    }

    private void parseAttrs(){
        Arrays.asList(this.attrs).forEach(attr -> {
            String[] values = attr.split("=");
            this.env.put(values[0], values[1]);
        });
        this.env.putIfAbsent(HOST, "localhost");
        this.env.putIfAbsent(PORT, "5432");
        this.env.putIfAbsent(DIR, System.getProperty("user.dir"));

        if (env.get(DBNAME) == null || env.get(USER) == null || env.get(PASS) == null) {
            System.err.println("Parameters: db, user and pass are required");
            System.err.println("Default value of parameter host is localhost");
            System.err.println("Default value of parameter port is 5432");
            System.err.println("Default value of parameter dir is current directory");
            System.err.println("Example parameters: dir=c:\\tmp host=localhost port=5432 db=test user=postgresql pass=secret");
            exit(0);
        }
    }

    private void enumerateFiles() {
        FilenameFilter ff = (dir, name) -> name.toLowerCase().endsWith(".sql");
        File[] files = new File(this.env.get(DIR)).listFiles(ff);
        if (files == null) {
            System.err.println("Invalid directory " + this.env.get(DIR));
            exit(0);
        }
        if (files.length == 0) {
            System.err.println("No files in directory " + this.env.get(DIR));
            exit(0);
        }
        this.filenames = Arrays.asList(files).stream().map(f -> f.getName()).collect(Collectors.toList());
        this.filenames.sort(String.CASE_INSENSITIVE_ORDER);
    }

    private void fetchExecutedFiles() {
        try {
            DBConnector db = new DBConnector(env.get(HOST), env.get(PORT), env.get(DBNAME), env.get(USER), env.get(PASS));
            this.executedFiles = db.getExecutedFiles();
        } catch (SQLException ex) {
            if (ex.getMessage().contains("public.sys_skrypty")) {
                System.out.println("New database");
            } else {
                System.err.println(ex.getMessage());
                exit(0);
            }
        }
    }

    private void processFiles(){
        this.filenames.forEach(file -> processFile(file));
    }

    private void processFile(String name) {
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(env.get(DIR), name));

            SortedMap<String, Charset> cs = Charset.availableCharsets();

            String script = new String(encoded, Charset.forName("windows-1250"));
            String fileCode = extractFileCode(script);
            if (fileCode.length() == 0) {
                System.err.println("Can't find file code in " + name);
                return;
            }
            if (!excludeFile(fileCode)){
                System.out.println("Executing " + name);
                executeScript(script);
            } else {
                System.out.println("Skipping " + name);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private String extractFileCode(String result) {
        int idx = result.toLowerCase().indexOf("f_sys_skrypt_dodaj('");
        if (idx < 0)
            return "";
        result = result.substring(idx+20);
        idx = result.indexOf("'");
        if (idx < 0)
            return "";
        return result.substring(0, idx);
    }

    private boolean excludeFile(String fileCode){
        return this.executedFiles.stream().anyMatch(code -> code.toLowerCase().equals(fileCode.toLowerCase()));
    }

    private void executeScript(String script) {
        DBConnector db = new DBConnector(env.get(HOST), env.get(PORT), env.get(DBNAME), env.get(USER), env.get(PASS));
        try {
            db.executeScript(script);
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            exit(0);
        }
    }

}
