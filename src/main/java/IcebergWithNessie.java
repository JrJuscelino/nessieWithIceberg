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

public class IcebergWithNessie {
  Schema schema;

  public IcebergWithNessie(Schema schema){
    this.schema = schema;
  }

  public DataFile createParquetFile(OutputFile outputFile) throws IOException {
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

  public Table createIcebergTable(NessieCatalog catalog, String databaseName, String tableName) {
    final TableIdentifier TABLE_IDENTIFIER = TableIdentifier
        .of(databaseName, tableName);
    catalog.buildTable(TABLE_IDENTIFIER, schema);
    catalog.newCreateTableTransaction(TABLE_IDENTIFIER, schema);
    catalog.createTable(TABLE_IDENTIFIER, schema).location();

    return catalog.loadTable(TABLE_IDENTIFIER);
  }

  public void dropIcebergTable(NessieCatalog catalog, String databaseName, String tableName) {
    final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(databaseName, tableName);
    catalog.dropTable(TABLE_IDENTIFIER, false);
  }

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
