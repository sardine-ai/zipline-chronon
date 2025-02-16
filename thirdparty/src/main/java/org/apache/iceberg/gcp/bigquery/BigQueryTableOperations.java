//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.iceberg.gcp.bigquery;

import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.Table;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.iceberg.bigquery.relocated.org.slf4j.Logger;
import com.google.cloud.iceberg.bigquery.relocated.org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.BaseMetastoreTableOperations.CommitStatus;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;

public final class BigQueryTableOperations extends BaseMetastoreTableOperations {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableOperations.class);
    public static final String TABLE_PROPERTIES_BQ_CONNECTION = "bq_connection";
    private final BigQueryClient client;
    private final FileIO fileIO;
    private final TableReference tableReference;
    private final Configuration conf;

    BigQueryTableOperations(BigQueryClient client, FileIO fileIO, String project, String dataset, String table, Configuration conf) {
        this.client = client;
        this.fileIO = fileIO;
        this.tableReference = (new TableReference()).setProjectId(project).setDatasetId(dataset).setTableId(table);
        this.conf = conf;
    }

    public void doRefresh() {
        String metadataLocation = null;

        try {
            metadataLocation = this.getMetadataLocationOrThrow(this.client.getTable(this.tableReference).getExternalCatalogTableOptions());
        } catch (NoSuchTableException e) {
            if (this.currentMetadataLocation() != null) {
                throw e;
            }
        }

        this.refreshFromMetadataLocation(metadataLocation);
    }

    public void doCommit(TableMetadata base, TableMetadata metadata) {
        String newMetadataLocation = base == null && metadata.metadataFileLocation() != null ? metadata.metadataFileLocation() : this.writeNewMetadata(metadata, this.currentVersion() + 1);
        BaseMetastoreTableOperations.CommitStatus commitStatus = CommitStatus.FAILURE;

        try {
            if (base == null) {
                this.createTable(newMetadataLocation, metadata);
            } else {
                this.updateTable(base.metadataFileLocation(), newMetadataLocation, metadata);
            }

            commitStatus = CommitStatus.SUCCESS;
        } catch (CommitStateUnknownException | CommitFailedException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error("Exception thrown on commit: ", e);
            commitStatus = this.checkCommitStatus(newMetadataLocation, metadata);
            if (commitStatus == CommitStatus.FAILURE) {
                throw new CommitFailedException(e, "Failed to commit", new Object[0]);
            }

            if (commitStatus == CommitStatus.UNKNOWN) {
                throw new CommitStateUnknownException(e);
            }
        } finally {
            try {
                if (commitStatus == CommitStatus.FAILURE) {
                    LOG.warn("Failed to commit updates to table {}", this.tableName());
                    this.io().deleteFile(newMetadataLocation);
                }
            } catch (RuntimeException e) {
                LOG.error("Failed to cleanup metadata file at {} for table {}", newMetadataLocation, e);
            }

        }

    }

    public String tableName() {
        return String.format("%s.%s", this.tableReference.getDatasetId(), this.tableReference.getTableId());
    }

    public FileIO io() {
        return this.fileIO;
    }

    private void createTable(String newMetadataLocation, TableMetadata metadata) {
        LOG.debug("Creating a new Iceberg table: {}", this.tableName());
        Table tableBuilder = this.makeNewTable(metadata, newMetadataLocation);
        tableBuilder.setTableReference(this.tableReference);
        this.addConnectionIfProvided(tableBuilder, metadata.properties());
        this.client.createTable(tableBuilder);
    }

    private void addConnectionIfProvided(Table tableBuilder, Map<String, String> metadataProperties) {
        if (metadataProperties.containsKey("bq_connection")) {
            tableBuilder.getExternalCatalogTableOptions().setConnectionId((String)metadataProperties.get("bq_connection"));
        }

    }

    private void updateTable(String oldMetadataLocation, String newMetadataLocation, TableMetadata metadata) {
        Table table = this.client.getTable(this.tableReference);
        if (table.getEtag().isEmpty()) {
            throw new ValidationException("Etag of legacy table %s is empty, manually update the table via the BigQuery API or recreate and retry", new Object[]{this.tableName()});
        } else {
            ExternalCatalogTableOptions options = table.getExternalCatalogTableOptions();
            this.addConnectionIfProvided(table, metadata.properties());
            String metadataLocationFromMetastore = (String)options.getParameters().getOrDefault("metadata_location", "");
            if (!metadataLocationFromMetastore.isEmpty() && !metadataLocationFromMetastore.equals(oldMetadataLocation)) {
                throw new CommitFailedException("Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s", new Object[]{oldMetadataLocation, metadataLocationFromMetastore, this.tableReference.getDatasetId(), this.tableReference.getTableId()});
            } else {
                options.setParameters(this.buildTableParameters(newMetadataLocation, metadata));

                try {
                    this.client.patchTable(this.tableReference, table);
                } catch (ValidationException e) {
                    if (e.getMessage().toLowerCase().contains("etag mismatch")) {
                        throw new CommitFailedException("Updating table failed due to conflict updates (etag mismatch). Retry the update", new Object[0]);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private Table makeNewTable(TableMetadata metadata, String metadataFileLocation) {
        boolean hiveEngineEnabled = this.getHiveEngineEnabled(metadata);
        return (new Table()).setExternalCatalogTableOptions(BigQueryMetastoreUtils.createExternalCatalogTableOptions(metadata.location(), this.buildTableParameters(metadataFileLocation, metadata), hiveEngineEnabled));
    }

    private Map<String, String> buildTableParameters(String metadataFileLocation, TableMetadata metadata) {
        Map<String, String> parameters = new HashMap(metadata.properties());
        if (metadata.uuid() != null) {
            parameters.put("uuid", metadata.uuid());
        }

        if (this.currentMetadataLocation() != null && !this.currentMetadataLocation().isEmpty()) {
            parameters.put("previous_metadata_location", this.currentMetadataLocation());
        }

        parameters.put("metadata_location", metadataFileLocation);
        parameters.put("table_type", "iceberg");
        parameters.put("EXTERNAL", "TRUE");
        updateParametersWithSnapshotMetadata(metadata, parameters);
        return parameters;
    }

    private static void updateParametersWithSnapshotMetadata(TableMetadata metadata, Map<String, String> parameters) {
        if (metadata.currentSnapshot() != null) {
            Map<String, String> summary = metadata.currentSnapshot().summary();
            if (summary.get("total-data-files") != null) {
                parameters.put("numFiles", (String)summary.get("total-data-files"));
            }

            if (summary.get("total-records") != null) {
                parameters.put("numRows", (String)summary.get("total-records"));
            }

            if (summary.get("total-files-size") != null) {
                parameters.put("totalSize", (String)summary.get("total-files-size"));
            }

        }
    }

    private String getMetadataLocationOrThrow(ExternalCatalogTableOptions tableOptions) {
        if (tableOptions != null && tableOptions.getParameters().containsKey("metadata_location")) {
            return (String)tableOptions.getParameters().get("metadata_location");
        } else {
            throw new ValidationException("Table %s is not a valid BigQuery Metastore Iceberg table, metadata location not found", new Object[]{this.tableName()});
        }
    }

    private boolean getHiveEngineEnabled(TableMetadata metadata) {
        return metadata.properties().get("engine.hive.enabled") != null ? metadata.propertyAsBoolean("engine.hive.enabled", false) : this.conf.getBoolean("iceberg.engine.hive.enabled", false);
    }
}
