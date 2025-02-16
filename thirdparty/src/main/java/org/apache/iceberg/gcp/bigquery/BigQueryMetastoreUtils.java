//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.iceberg.gcp.bigquery;

import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.SerDeInfo;
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.StorageDescriptor;
import java.util.Map;

public final class BigQueryMetastoreUtils {
    public static final String HIVE_SERIALIZATION_LIBRARY = "org.apache.iceberg.mr.hive.HiveIcebergSerDe";
    public static final String HIVE_FILE_INPUT_FORMAT = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
    public static final String HIVE_FILE_OUTPUT_FORMAT = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";
    public static final String SERIALIZATION_LIBRARY = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    public static final String FILE_INPUT_FORMAT = "org.apache.hadoop.mapred.FileInputFormat";
    public static final String FILE_OUTPUT_FORMAT = "org.apache.hadoop.mapred.FileOutputFormat";

    private BigQueryMetastoreUtils() {
    }

    public static ExternalCatalogTableOptions createExternalCatalogTableOptions(String locationUri, Map<String, String> parameters, boolean hiveEngineEnabled) {
        return (new ExternalCatalogTableOptions()).setStorageDescriptor(createStorageDescriptor(locationUri, hiveEngineEnabled)).setParameters(parameters);
    }

    public static ExternalCatalogDatasetOptions createExternalCatalogDatasetOptions(String defaultStorageLocationUri, Map<String, String> metadataParameters) {
        return (new ExternalCatalogDatasetOptions()).setDefaultStorageLocationUri(defaultStorageLocationUri).setParameters(metadataParameters);
    }

    private static StorageDescriptor createStorageDescriptor(String locationUri, boolean hiveEngineEnabled) {
        String inputFormat;
        String outputFormat;
        String serializationLibrary;
        if (hiveEngineEnabled) {
            serializationLibrary = "org.apache.iceberg.mr.hive.HiveIcebergSerDe";
            inputFormat = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
            outputFormat = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";
        } else {
            serializationLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
            inputFormat = "org.apache.hadoop.mapred.FileInputFormat";
            outputFormat = "org.apache.hadoop.mapred.FileOutputFormat";
        }

        SerDeInfo serDeInfo = (new SerDeInfo()).setSerializationLibrary(serializationLibrary);
        return (new StorageDescriptor()).setLocationUri(locationUri).setInputFormat(inputFormat).setOutputFormat(outputFormat).setSerdeInfo(serDeInfo);
    }
}
