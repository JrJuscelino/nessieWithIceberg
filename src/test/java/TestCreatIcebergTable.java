import static org.hamcrest.CoreMatchers.is;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Reference;

public class TestCreatIcebergTable {
  final static String nessieURI = "http://localhost:19120/api/v1";
  final static String nessieBranchName = "test";
  final static String databaseName = "database_sample";
  final static String tableName = "table_sample";
  final Schema schema = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));
  static Reference nessieBranch;
  static NessieClient nessieClient;
  static TreeApi tree;
  static String warehousePath;

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setUp() throws IOException {
    nessieClient = NessieClient.builder().withUri(nessieURI).build();
    tree = nessieClient.getTreeApi();
    nessieBranch = tree.createReference(Branch.of(nessieBranchName, null));
    File tmpDir = File.createTempFile("my_prefix", "");
    tmpDir.delete();
    tmpDir.mkdir();
    warehousePath = tmpDir.toString();
  }

  @AfterClass
  public static void tearDown() throws Exception {;
    for (Reference r : tree.getAllReferences()) {
      if (r instanceof Branch && Objects.equals(r.getName(), nessieBranchName)) {
        tree.deleteBranch(r.getName(), r.getHash());
        break;
      }
    }

    nessieClient.close();
  }

  private void verifyCommitMetadata() throws NessieNotFoundException {
    NessieClient client = NessieClient.builder().withUri(nessieURI).build();
    TreeApi tree = client.getTreeApi();
    List<CommitMeta> log = tree.getCommitLog(nessieBranchName, CommitLogParams.empty()).getOperations();
    collector.checkThat(log.isEmpty(), is(false));
    log.forEach(x -> {
      collector.checkThat(x.getAuthor(), is(System.getProperty("user.name")));
    });
  }

  @Test
  public void testCreateParquetFile() throws IOException {
    OutputFile file = Files.localOutput(temp.newFile());

    IcebergWithNessie object = new IcebergWithNessie(schema);
    DataFile parquetFile = object.createParquetFile(file);

    collector.checkThat(parquetFile.format(), is(FileFormat.PARQUET));
    collector.checkThat(parquetFile.content(), is(FileContent.DATA));
  }

  @Test
  public void testCreateAndDropIcebergTable() throws NessieNotFoundException {
    IcebergWithNessie object = new IcebergWithNessie(schema);
    NessieCatalog catalog = object.initializeNessieCatalog(nessieURI, nessieBranchName, warehousePath);
    Table icebergTable = object.createIcebergTable(catalog, databaseName, tableName);

    collector.checkThat(icebergTable.name(), is(String.format("nessie.%s.%s", databaseName, tableName)));

    object.dropIcebergTable(catalog, databaseName, tableName);

    collector.checkThat(catalog.tableExists(TableIdentifier.of(databaseName, tableName)), is(false));

    verifyCommitMetadata();
  }
}
