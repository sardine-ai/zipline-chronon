//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.iceberg.gcp.bigquery;

import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.Dataset;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.DatasetList;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.DatasetReference;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.iceberg.bigquery.relocated.com.google.common.annotations.VisibleForTesting;
import com.google.cloud.iceberg.bigquery.relocated.com.google.common.base.Preconditions;
import com.google.cloud.iceberg.bigquery.relocated.com.google.common.base.Strings;
import com.google.cloud.iceberg.bigquery.relocated.com.google.common.collect.ImmutableList;
import com.google.cloud.iceberg.bigquery.relocated.com.google.common.collect.ImmutableMap;
import com.google.cloud.iceberg.bigquery.relocated.org.slf4j.Logger;
import com.google.cloud.iceberg.bigquery.relocated.org.slf4j.LoggerFactory;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;

public final class BigQueryMetastoreCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {
    public static final String PROPERTIES_KEY_GCP_PROJECT = "gcp_project";
    public static final String PROPERTIES_KEY_GCP_LOCATION = "gcp_location";
    public static final String PROPERTIES_KEY_FILTER_UNSUPPORTED_TABLES = "filter_unsupported_tables";
    public static final String HIVE_METASTORE_WAREHOUSE_DIR = "hive.metastore.warehouse.dir";
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetastoreCatalog.class);
    private static final String DEFAULT_GCP_LOCATION = "us";
    private String catalogPluginName;
    private Map<String, String> catalogProperties;
    private FileIO fileIO;
    private Configuration conf;
    private String projectId;
    private String location;
    private BigQueryClient client;
    private boolean filterUnsupportedTables;

    public BigQueryMetastoreCatalog() {
    }

    public void initialize(String inputName, Map<String, String> properties) {
        if (!properties.containsKey("gcp_project")) {
            throw new ValidationException("GCP project must be specified", new Object[0]);
        } else {
            String projectId = (String)properties.get("gcp_project");
            String location = (String)properties.getOrDefault("gcp_location", "us");

            BigQueryClient client;
            try {
                client = new BigQueryClientImpl();
            } catch (IOException e) {
                throw new ServiceFailureException(e, "Creating BigQuery client failed", new Object[0]);
            } catch (GeneralSecurityException e) {
                throw new ValidationException(e, "Creating BigQuery client failed due to a security issue", new Object[0]);
            }

            this.initialize(inputName, properties, projectId, location, client);
        }
    }

    @VisibleForTesting
    void initialize(String inputName, Map<String, String> properties, String projectId, String location, BigQueryClient bigQueryClient) {
        this.catalogPluginName = inputName;
        this.catalogProperties = ImmutableMap.copyOf(properties);
        this.projectId = projectId;
        this.location = location;
        this.client = (BigQueryClient)Preconditions.checkNotNull(bigQueryClient);
        if (this.conf == null) {
            LOG.warn("No configuration was set, using the default environment Configuration");
            this.conf = new Configuration();
        }

        LOG.info("Using BigQuery Metastore Iceberg Catalog: {}", inputName);
        if (properties.containsKey("warehouse")) {
            this.conf.set("hive.metastore.warehouse.dir", LocationUtil.stripTrailingSlash((String)properties.get("warehouse")));
        }

        String fileIoImpl = (String)properties.getOrDefault("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");
        this.fileIO = CatalogUtil.loadFileIO(fileIoImpl, properties, this.conf);
        this.filterUnsupportedTables = Boolean.parseBoolean((String)properties.getOrDefault("filter_unsupported_tables", "false"));
    }

    protected TableOperations newTableOps(TableIdentifier identifier) {
        return new BigQueryTableOperations(this.client, this.fileIO, this.projectId, identifier.namespace().level(0), identifier.name(), this.conf);
    }

    protected String defaultWarehouseLocation(TableIdentifier identifier) {
        String locationUri = null;
        DatasetReference datasetReference = this.toDatasetReference(identifier.namespace());
        Dataset dataset = this.client.getDataset(datasetReference);
        if (dataset != null && dataset.getExternalCatalogDatasetOptions() != null) {
            locationUri = dataset.getExternalCatalogDatasetOptions().getDefaultStorageLocationUri();
        }

        return String.format("%s/%s", Strings.isNullOrEmpty(locationUri) ? this.getDefaultStorageLocationUri(datasetReference.getDatasetId()) : locationUri, identifier.name());
    }

    public List<TableIdentifier> listTables(Namespace namespace) {
        validateNamespace(namespace);
        return (List)this.client.listTables(this.toDatasetReference(namespace), this.filterUnsupportedTables).stream().map((table) -> TableIdentifier.of(new String[]{namespace.level(0), table.getTableReference().getTableId()})).collect(ImmutableList.toImmutableList());
    }

    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        try {
            TableOperations ops = this.newTableOps(identifier);
            TableMetadata lastMetadata = ops.current();
            this.client.deleteTable(this.toBqTableReference(identifier));
            if (purge && lastMetadata != null) {
                CatalogUtil.dropTableData(ops.io(), lastMetadata);
            }

            return true;
        } catch (NoSuchTableException var5) {
            return false;
        }
    }

    public void renameTable(TableIdentifier from, TableIdentifier to) {
        if (!from.namespace().equals(to.namespace())) {
            throw new ValidationException("New table name must be in the same namespace", new Object[0]);
        } else {
            throw new ServiceFailureException("Table rename operation is unsupported.", new Object[0]);
        }
    }

    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        Dataset builder = new Dataset();
        DatasetReference datasetReference = this.toDatasetReference(namespace);
        builder.setLocation(this.location);
        builder.setDatasetReference(datasetReference);
        builder.setExternalCatalogDatasetOptions(BigQueryMetastoreUtils.createExternalCatalogDatasetOptions(this.getDefaultStorageLocationUri(datasetReference.getDatasetId()), metadata));
        this.client.createDataset(builder);
    }

    public List<Namespace> listNamespaces(Namespace namespace) {
        return (List<Namespace>)(namespace.levels().length != 0 ? ImmutableList.of() : (List)this.client.listDatasets(this.projectId).stream().map(BigQueryMetastoreCatalog::getNamespace).collect(ImmutableList.toImmutableList()));
    }

    public boolean dropNamespace(Namespace namespace) {
        this.client.deleteDataset(this.toDatasetReference(namespace));
        return true;
    }

    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        this.client.setDatasetParameters(this.toDatasetReference(namespace), properties);
        return true;
    }

    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        this.client.removeDatasetParameters(this.toDatasetReference(namespace), properties);
        return true;
    }

    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        return getMetadata(this.client.getDataset(this.toDatasetReference(namespace)));
    }

    public String name() {
        return this.catalogPluginName;
    }

    protected Map<String, String> properties() {
        return (Map<String, String>)(this.catalogProperties == null ? ImmutableMap.of() : this.catalogProperties);
    }

    public void setConf(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    public Configuration getConf() {
        return this.conf;
    }

    private String getDefaultStorageLocationUri(String dbId) {
        String warehouseLocation = this.conf.get("hive.metastore.warehouse.dir");
        Preconditions.checkNotNull(warehouseLocation, "Data warehouse location is not set");
        return String.format("%s/%s.db", LocationUtil.stripTrailingSlash(warehouseLocation), dbId);
    }

    private static Namespace getNamespace(DatasetList.Datasets datasets) {
        return Namespace.of(new String[]{datasets.getDatasetReference().getDatasetId()});
    }

    private DatasetReference toDatasetReference(Namespace namespace) {
        validateNamespace(namespace);
        return (new DatasetReference()).setProjectId(this.projectId).setDatasetId(namespace.level(0));
    }

    private TableReference toBqTableReference(TableIdentifier tableIdentifier) {
        DatasetReference datasetReference = this.toDatasetReference(tableIdentifier.namespace());
        return (new TableReference()).setProjectId(datasetReference.getProjectId()).setDatasetId(datasetReference.getDatasetId()).setTableId(tableIdentifier.name());
    }

    private static Map<String, String> getMetadata(Dataset dataset) {
        final ExternalCatalogDatasetOptions options = dataset.getExternalCatalogDatasetOptions();
        return new HashMap<String, String>() {
            {
                if (options != null) {
                    if (options.getParameters() != null) {
                        this.putAll(options.getParameters());
                    }

                    if (!Strings.isNullOrEmpty(options.getDefaultStorageLocationUri())) {
                        this.put("location", options.getDefaultStorageLocationUri());
                    }
                }

            }
        };
    }

    private static void validateNamespace(Namespace namespace) {
        Preconditions.checkArgument(namespace.levels().length == 1, invalidNamespaceMessage(namespace));
    }

    private static String invalidNamespaceMessage(Namespace namespace) {
        return String.format("BigQuery Metastore only supports single level namespaces. Invalid namespace: \"%s\" has %d levels", namespace, namespace.levels().length);
    }
}
