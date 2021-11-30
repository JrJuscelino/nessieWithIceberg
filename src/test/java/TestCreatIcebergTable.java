import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;
import java.util.Objects;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

public class TestCreatIcebergTable {
  final static String nessieURI = "http://localhost:19120/api/v1";
  final static String nessieBranchName = "test";
  final static String databaseName = "database_sample";
  final static String tableName = "table_sample";
  final static String warehousePath = "hdfs://localhost:9000/warehouse";
  final Schema schema = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));
  static Reference nessieBranch;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setUp() throws NessieConflictException, NessieNotFoundException {
    NessieClient client = NessieClient.builder().withUri(nessieURI).build();
    TreeApi tree = client.getTreeApi();
    nessieBranch = tree.createReference(Branch.of(nessieBranchName, null));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    NessieClient client = NessieClient.builder().withUri(nessieURI).build();
    TreeApi tree = client.getTreeApi();
    for (Reference r : tree.getAllReferences()) {
      if (r instanceof Branch && Objects.equals(r.getName(), nessieBranchName)) {
        tree.deleteBranch(r.getName(), r.getHash());
        break;
      }
    }
  }

  @Test
  public void testCreateParquetFile() throws IOException {
    OutputFile file = Files.localOutput(temp.newFile());

    IcebergwithNessie object = new IcebergwithNessie(schema);
    DataFile parquetFile = object.createParquetFile(file);

    collector.checkThat(parquetFile.format(), is(FileFormat.PARQUET));
    collector.checkThat(parquetFile.content(), is(FileContent.DATA));
  }

  @Test
  public void testCreateIcebergTable() {
    IcebergwithNessie object = new IcebergwithNessie(schema);
    Table icebergTable =
        object.createIcebergTable(nessieURI, nessieBranchName, databaseName, tableName, warehousePath);

    collector.checkThat(icebergTable.name(), is(String.format("nessie.database_sample.%s", tableName)));
  }
}
