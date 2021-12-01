import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;

/**
 *  This class provides the methods that create a parquet file and create and drop an iceberg table using a Nessie catalog.
 *
 */
public class IcebergWithNessie {
  @RegisterExtension
  static NessieJaxRsExtension server = new NessieJaxRsExtension();

  Schema schema;

  /**
   * Create a new {@link IcebergWithNessie} instance.
   * @param schema represent the schema that will be used to create the parquet files and the Iceberg tables
   */
  public IcebergWithNessie(Schema schema){
    this.schema = schema;
  }

  /**
   * Create a parquet file in a defined directory.
   * @param outputFile represent the file
   * @return The parquet file in datafile format
   */
  public DataFile writeParquetFile(OutputFile outputFile) throws IOException {
    GenericRecord record = GenericRecord.create(schema);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(record.copy(ImmutableMap.of("id", 1L, "data", "a")));
    builder.add(record.copy(ImmutableMap.of("id", 2L, "data", "b")));
    builder.add(record.copy(ImmutableMap.of("id", 3L, "data", "c")));
    builder.add(record.copy(ImmutableMap.of("id", 4L, "data", "d")));
    builder.add(record.copy(ImmutableMap.of("id", 5L, "data", "e")));

    List<Record> records = builder.build();

    SortOrder sortOrder = SortOrder.builderFor(schema)
        .withOrderId(10)
        .asc("id")
        .build();

    DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .withSortOrder(sortOrder)
        .build();

    try {
      for (Record r : records) {
        dataWriter.add(r);
      }
    } finally {
      dataWriter.close();
    }

    return dataWriter.toDataFile();
  }

  /**
   * Create an Iceberg table using a Nessie catalog.
   * @param catalog represent a initialized Nessie catalog
   * @param databaseName the defined database name where the table will be created
   * @param tableName define name to the table to be created
   * @return load an iceberg table from Nessie catalog
   */
  public Table createIcebergTable(NessieCatalog catalog, String databaseName, String tableName) {
    final TableIdentifier TABLE_IDENTIFIER = TableIdentifier
        .of(databaseName, tableName);
    catalog.buildTable(TABLE_IDENTIFIER, schema);
    catalog.newCreateTableTransaction(TABLE_IDENTIFIER, schema);
    catalog.createTable(TABLE_IDENTIFIER, schema).location();

    return catalog.loadTable(TABLE_IDENTIFIER);
  }

  /**
   * Drop an Iceberg table.
   * @param catalog represent a initialized Nessie catalog
   * @param databaseName the database that contains the table that will be dropped
   * @param tableName the name of the table that will be dropped
   */
  public void dropIcebergTable(NessieCatalog catalog, String databaseName, String tableName) {
    final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(databaseName, tableName);
    catalog.dropTable(TABLE_IDENTIFIER, false);
  }

  /**
   * Initialize a Nessie catalog.
   * @param NessieURI uri of the Nessie server
   * @param branchName define the branch name that will be used as a reference by the catalog
   * @param warehousePath path where the metadata files will be saved
   * @return represent a initialized Nessie catalog
   */
  public NessieCatalog initializeNessieCatalog(String NessieURI, String branchName, String warehousePath){
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

    NessieCatalog catalog = new NessieCatalog();
    catalog.setConf(hadoopConfig);
    catalog.initialize("nessie", ImmutableMap.of("ref", branchName,
        CatalogProperties.URI, NessieURI,
        "auth-type", "NONE",
        CatalogProperties.WAREHOUSE_LOCATION, warehousePath
    ));

    return catalog;
  }
}
