//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.iceberg.gcp.bigquery;

import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.Dataset;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.DatasetList;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.DatasetReference;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.Table;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableList;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableReference;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BigQueryClient {
    Dataset createDataset(Dataset var1);

    Dataset getDataset(DatasetReference var1);

    void deleteDataset(DatasetReference var1);

    Dataset setDatasetParameters(DatasetReference var1, Map<String, String> var2);

    Dataset removeDatasetParameters(DatasetReference var1, Set<String> var2);

    List<DatasetList.Datasets> listDatasets(String var1);

    Table createTable(Table var1);

    Table getTable(TableReference var1);

    Table patchTable(TableReference var1, Table var2);

    Table renameTable(TableReference var1, String var2);

    void deleteTable(TableReference var1);

    List<TableList.Tables> listTables(DatasetReference var1, boolean var2);
}
