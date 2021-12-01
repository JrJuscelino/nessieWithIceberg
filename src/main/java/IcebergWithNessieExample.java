import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.types.Types;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.client.NessieClient;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Reference;

public class IcebergWithNessieExample {
  public static void main(String[] args) throws IOException {
    String nessieURI = null;
    String nessieBranchName = null;
    String databaseName = null;
    String tableName = null;
    String warehousePath = null;
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

    } catch (IOException ex) {
      ex.printStackTrace();
    }

    nessieClient = NessieClient.builder().withUri(nessieURI).build();
    tree = nessieClient.getTreeApi();
    tree.createReference(Branch.of(nessieBranchName, null));

    final Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));

    File tempFile = File.createTempFile("icebergExample-", ".tmp");
    OutputFile outputFile = Files.localOutput(tempFile);

    IcebergWithNessie icebergWithNessie = new IcebergWithNessie(schema);

    DataFile parquetFile = icebergWithNessie.createParquetFile(outputFile);
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
