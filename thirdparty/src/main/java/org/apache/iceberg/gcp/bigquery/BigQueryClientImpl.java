//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.iceberg.gcp.bigquery;

import com.google.cloud.iceberg.bigquery.relocated.com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.client.http.HttpHeaders;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.client.http.HttpResponse;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.client.http.HttpResponseException;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.client.json.gson.GsonFactory;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.client.util.Data;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.Bigquery;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.BigqueryScopes;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.Dataset;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.DatasetList;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.DatasetReference;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.Table;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableList;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.iceberg.bigquery.relocated.com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.iceberg.bigquery.relocated.com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iceberg.bigquery.relocated.com.google.common.annotations.VisibleForTesting;
import com.google.cloud.iceberg.bigquery.relocated.com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.ValidationException;

public final class BigQueryClientImpl implements BigQueryClient {
    private final Bigquery client;

    public BigQueryClientImpl() throws IOException, GeneralSecurityException {
        HttpCredentialsAdapter httpCredentialsAdapter = new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault().createScoped(BigqueryScopes.all()));
        this.client = (new Bigquery.Builder(GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance(), (httpRequest) -> {
            httpCredentialsAdapter.initialize(httpRequest);
            httpRequest.setThrowExceptionOnExecuteError(false);
        })).setApplicationName("BigQuery Iceberg Catalog Plugin").build();
    }

    @VisibleForTesting
    BigQueryClientImpl(Bigquery client) {
        this.client = client;
    }

    public Dataset createDataset(Dataset dataset) {
        try {
            HttpResponse response = this.client.datasets().insert(dataset.getDatasetReference().getProjectId(), dataset).executeUnparsed();
            return (Dataset)this.convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public Dataset getDataset(DatasetReference datasetReference) {
        try {
            HttpResponse response = this.client.datasets().get(datasetReference.getProjectId(), datasetReference.getDatasetId()).executeUnparsed();
            if (response.getStatusCode() == 404) {
                throw new NoSuchNamespaceException(response.getStatusMessage(), new Object[0]);
            } else {
                return (Dataset)this.convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public void deleteDataset(DatasetReference datasetReference) {
        try {
            HttpResponse response = this.client.datasets().delete(datasetReference.getProjectId(), datasetReference.getDatasetId()).executeUnparsed();
            if (response.getStatusCode() == 404) {
                throw new NoSuchNamespaceException(response.getStatusMessage(), new Object[0]);
            } else {
                this.convertExceptionIfUnsuccessful(response);
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public Dataset setDatasetParameters(DatasetReference datasetReference, Map<String, String> parameters) {
        Dataset dataset = this.getDataset(datasetReference);
        ExternalCatalogDatasetOptions externalCatalogDatasetOptions = dataset.getExternalCatalogDatasetOptions() == null ? new ExternalCatalogDatasetOptions() : dataset.getExternalCatalogDatasetOptions();
        Map<String, String> finalParameters = (Map<String, String>)(externalCatalogDatasetOptions.getParameters() == null ? new HashMap() : externalCatalogDatasetOptions.getParameters());
        finalParameters.putAll(parameters);
        dataset.setExternalCatalogDatasetOptions(externalCatalogDatasetOptions.setParameters(finalParameters));
        return this.updateDataset(dataset);
    }

    public Dataset removeDatasetParameters(DatasetReference datasetReference, Set<String> parameters) {
        Dataset dataset = this.getDataset(datasetReference);
        ExternalCatalogDatasetOptions externalCatalogDatasetOptions = dataset.getExternalCatalogDatasetOptions() == null ? new ExternalCatalogDatasetOptions() : dataset.getExternalCatalogDatasetOptions();
        Map<String, String> finalParameters = (Map<String, String>)(externalCatalogDatasetOptions.getParameters() == null ? new HashMap() : externalCatalogDatasetOptions.getParameters());
        Objects.requireNonNull(finalParameters);
        parameters.forEach(finalParameters::remove);
        dataset.setExternalCatalogDatasetOptions(externalCatalogDatasetOptions.setParameters(finalParameters));
        return this.updateDataset(dataset);
    }

    public List<DatasetList.Datasets> listDatasets(String projectId) {
        try {
            String nextPageToken = null;
            List<DatasetList.Datasets> datasets = new ArrayList();

            do {
                HttpResponse pageResponse = this.client.datasets().list(projectId).setPageToken(nextPageToken).executeUnparsed();
                DatasetList result = (DatasetList)this.convertExceptionIfUnsuccessful(pageResponse).parseAs(DatasetList.class);
                nextPageToken = result.getNextPageToken();
                if (result.getDatasets() != null) {
                    datasets.addAll(result.getDatasets());
                }
            } while(nextPageToken != null && !nextPageToken.isEmpty());

            return datasets;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public Table createTable(Table table) {
        try {
            this.validateTable(table);
            HttpResponse response = this.client.tables().insert(((TableReference)Preconditions.checkNotNull(table.getTableReference())).getProjectId(), ((TableReference)Preconditions.checkNotNull(table.getTableReference())).getDatasetId(), table).executeUnparsed();
            return (Table)this.convertExceptionIfUnsuccessful(response).parseAs(Table.class);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public Table getTable(TableReference tableReference) {
        try {
            HttpResponse response = this.client.tables().get(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId()).executeUnparsed();
            if (response.getStatusCode() == 404) {
                throw new NoSuchTableException(response.getStatusMessage(), new Object[0]);
            } else {
                return this.validateTable((Table)this.convertExceptionIfUnsuccessful(response).parseAs(Table.class));
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public Table patchTable(TableReference tableReference, Table table) {
        this.validateTable(table);
        ExternalCatalogTableOptions newExternalCatalogTableOptions = (new ExternalCatalogTableOptions()).setStorageDescriptor(table.getExternalCatalogTableOptions().getStorageDescriptor()).setConnectionId(table.getExternalCatalogTableOptions().getConnectionId()).setParameters(table.getExternalCatalogTableOptions().getParameters());
        Table patch = (new Table()).setExternalCatalogTableOptions(newExternalCatalogTableOptions).setSchema((TableSchema)Data.nullOf(TableSchema.class));

        try {
            HttpResponse response = this.client.tables().patch(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId(), patch).setRequestHeaders((new HttpHeaders()).setIfMatch(table.getEtag())).executeUnparsed();
            if (response.getStatusCode() == 404) {
                String responseString = response.parseAsString();
                if (responseString.toLowerCase().contains("not found: connection")) {
                    throw new BadRequestException(responseString, new Object[0]);
                } else {
                    throw new NoSuchTableException(response.getStatusMessage(), new Object[0]);
                }
            } else {
                return (Table)this.convertExceptionIfUnsuccessful(response).parseAs(Table.class);
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public Table renameTable(TableReference tableToRename, String newTableId) {
        Table table = this.getTable(tableToRename);
        Table patch = (new Table()).setTableReference((new TableReference()).setProjectId(table.getTableReference().getProjectId()).setDatasetId(table.getTableReference().getDatasetId()).setTableId(newTableId));

        try {
            HttpResponse response = this.client.tables().patch(tableToRename.getProjectId(), tableToRename.getDatasetId(), tableToRename.getTableId(), patch).setRequestHeaders((new HttpHeaders()).setIfMatch(table.getEtag())).executeUnparsed();
            if (response.getStatusCode() == 404) {
                throw new NoSuchTableException(response.getStatusMessage(), new Object[0]);
            } else {
                return (Table)this.convertExceptionIfUnsuccessful(response).parseAs(Table.class);
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public void deleteTable(TableReference tableReference) {
        try {
            this.getTable(tableReference);
            HttpResponse response = this.client.tables().delete(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId()).executeUnparsed();
            if (response.getStatusCode() == 404) {
                throw new NoSuchTableException(response.getStatusMessage(), new Object[0]);
            } else {
                this.convertExceptionIfUnsuccessful(response);
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public List<TableList.Tables> listTables(DatasetReference datasetReference, boolean filterUnsupportedTables) {
        try {
            String nextPageToken = null;
            Stream<TableList.Tables> tablesStream = Stream.of();

            do {
                HttpResponse pageResponse = this.client.tables().list(datasetReference.getProjectId(), datasetReference.getDatasetId()).setPageToken(nextPageToken).executeUnparsed();
                if (pageResponse.getStatusCode() == 404) {
                    throw new NoSuchNamespaceException(pageResponse.getStatusMessage(), new Object[0]);
                }

                TableList result = (TableList)this.convertExceptionIfUnsuccessful(pageResponse).parseAs(TableList.class);
                nextPageToken = result.getNextPageToken();
                List<TableList.Tables> tablesPage = result.getTables();
                Stream<TableList.Tables> tablesPageStream = tablesPage == null ? Stream.of() : result.getTables().stream();
                tablesStream = Stream.concat(tablesStream, tablesPageStream);
            } while(nextPageToken != null && !nextPageToken.isEmpty());

            if (filterUnsupportedTables) {
                tablesStream = ((Stream)tablesStream.parallel()).filter((table) -> {
                    try {
                        this.getTable(table.getTableReference());
                        return true;
                    } catch (NoSuchTableException var3) {
                        return false;
                    }
                });
            }

            return (List)tablesStream.collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private Dataset updateDataset(Dataset dataset) {
        try {
            HttpResponse response = this.client.datasets().update(((DatasetReference)Preconditions.checkNotNull(dataset.getDatasetReference())).getProjectId(), (String)Preconditions.checkNotNull(dataset.getDatasetReference().getDatasetId()), dataset).setRequestHeaders((new HttpHeaders()).setIfMatch(dataset.getEtag())).executeUnparsed();
            if (response.getStatusCode() == 404) {
                throw new NoSuchNamespaceException(response.getStatusMessage(), new Object[0]);
            } else {
                return (Dataset)this.convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private boolean isValidIcebergTable(Table table) {
        return table.getExternalCatalogTableOptions() != null && !table.getExternalCatalogTableOptions().isEmpty() && table.getExternalCatalogTableOptions().getParameters() != null && table.getExternalCatalogTableOptions().getParameters().containsKey("metadata_location") && "iceberg".equalsIgnoreCase((String)table.getExternalCatalogTableOptions().getParameters().get("table_type"));
    }

    private Table validateTable(Table table) {
        if (!this.isValidIcebergTable(table)) {
            throw new NoSuchIcebergTableException("This table is not a valid Iceberg table: %s", new Object[]{table});
        } else {
            return table;
        }
    }

    private HttpResponse convertExceptionIfUnsuccessful(HttpResponse response) throws IOException {
        if (response.isSuccessStatusCode()) {
            return response;
        } else {
            String errorMessage = String.format("%s\n%s", response.getStatusMessage(), response.getContent() != null ? new String(response.getContent().readAllBytes()) : "");
            switch (response.getStatusCode()) {
                case 400:
                    throw new BadRequestException(errorMessage, new Object[0]);
                case 401:
                    throw new NotAuthorizedException(errorMessage, new Object[]{"Not authorized to call the BigQuery API or access this resource"});
                case 403:
                    throw new ForbiddenException(errorMessage, new Object[0]);
                case 404:
                    throw new NotFoundException(errorMessage, new Object[0]);
                case 412:
                    throw new ValidationException(errorMessage, new Object[0]);
                case 500:
                    throw new ServiceFailureException(errorMessage, new Object[0]);
                case 503:
                    throw new ServiceUnavailableException(errorMessage, new Object[0]);
                default:
                    throw new HttpResponseException(response);
            }
        }
    }
}
