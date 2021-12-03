import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.client.NessieClient;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Reference;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.iceberg.Files.localOutput;

public class IcebergWithNessieExample {
  public static void main(String[] args) throws IOException {
    String nessieURI = null;
    String nessieBranchName = null;
    String databaseName = null;
    String tableName = null;
    String warehousePath = null;
    String schemaPath = null;
    String propFileName = "config.properties";
    NessieClient nessieClient;
    TreeApi tree;

    // load a properties file
    try (InputStream input = IcebergWithNessieExample.class.getClassLoader().getResourceAsStream(propFileName)) {
      Properties properties = new Properties();

      if (input == null) {
        System.out.println("Unable to find config.properties");
        return;
      }

      properties.load(input);

      nessieURI = properties.getProperty("nessieURI");
      nessieBranchName = properties.getProperty("nessieBranchName");
      databaseName = properties.getProperty("databaseName");
      tableName = properties.getProperty("tableName");
      warehousePath = properties.getProperty("warehousePath");
      schemaPath = properties.getProperty("schemaPath");

    } catch (IOException ex) {
      ex.printStackTrace();
    }

    nessieClient = NessieClient.builder().withUri(nessieURI).build();
    tree = nessieClient.getTreeApi();
  //  tree.createReference(Branch.of(nessieBranchName, null));

    File tempFile = File.createTempFile("icebergExample-", ".parquet");
    OutputFile outputFile = localOutput(tempFile);

    File resource = new File(schemaPath);
    String rawSchema = new String(Files.readAllBytes(resource.toPath()));
    MessageType parquetSchema =  MessageTypeParser.parseMessageType(rawSchema);
    IcebergWithNessie icebergWithNessie = new IcebergWithNessie(parquetSchema);

    HashMap<String, Object> parquetData = new HashMap<>();
    parquetData.put("id", 1);
    parquetData.put("data", "value");
    DataFile parquetFile = icebergWithNessie.writeParquetFile(outputFile, parquetData);
    System.out.printf("File format: %s%n", parquetFile.format());
    System.out.printf("Record count: %s%n", parquetFile.recordCount());

    NessieCatalog catalog = icebergWithNessie.initializeNessieCatalog(nessieURI, nessieBranchName, warehousePath);
    Table icebergTable = icebergWithNessie.createIcebergTable(catalog, databaseName, tableName);

    System.out.printf("Table: %s%n", icebergTable.name());

    icebergWithNessie.dropIcebergTable(catalog, databaseName, tableName);

    List<CommitMeta> log = tree.getCommitLog(nessieBranchName, CommitLogParams.empty()).getOperations();

    for (CommitMeta commitMeta : log) {
      System.out.println(commitMeta);
    }

    for (Reference r : tree.getAllReferences()) {
      if (r instanceof Branch && Objects.equals(r.getName(), nessieBranchName)) {
        tree.deleteBranch(r.getName(), r.getHash());
        break;
      }
    }

    tempFile.delete();
  }
}
