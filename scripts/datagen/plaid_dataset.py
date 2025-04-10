from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
    DoubleType,
    LongType,
    TimestampType,
)
from pyspark.sql import SparkSession
import random
import datetime

table_schema = {
    # "_hoodie_commit_time": ("string", "True"),
    # "_hoodie_commit_seqno": ("string", "True"),
    # "_hoodie_record_key": ("string", "True"),
    # "_hoodie_partition_path": ("string", "True"),
    # "_hoodie_file_name": ("string", "True"),
    "browser_name": ("string", "True"),
    "link_warden_user_agent_hash_matches_user_agent_from_request": ("int", "True"),
    "link_warden_webdriver_present": ("int", "True"),
    "user_agent_browser": ("string", "True"),
    "user_agent_device": ("string", "True"),
    "user_agent_device_brand": ("string", "True"),
    "user_agent_device_model": ("string", "True"),
    "user_agent_family": ("string", "True"),
    "user_agent_os": ("string", "True"),
    "user_agent_os_family": ("string", "True"),
    "user_agent_os_version_major": ("string", "True"),
    "browser_version": ("string", "True"),
    "user_agent_value": ("string", "True"),
    "request_ip_v4_address": ("string", "True"),
    "fingerprint_pro_data_ip_v4_accuracy_radius": ("bigint", "True"),
    "fingerprint_pro_data_ip_v4_asn_name": ("string", "True"),
    "fingerprint_pro_data_ip_v4_city": ("string", "True"),
    "fingerprint_pro_data_ip_v4_continent_name": ("string", "True"),
    "fingerprint_pro_data_ip_v4_country_name": ("string", "True"),
    "fingerprint_pro_data_ip_v4_datacenter_ip": ("int", "True"),
    "fingerprint_pro_data_ip_v4_datacenter_name": ("string", "True"),
    "fingerprint_pro_data_ip_v4_postal_code": ("string", "True"),
    "client_id": ("string", "True"),
    "fingerprint_pro_data_ip_v4_timezone": ("string", "True"),
    "fingerprint_pro_data_ip_v4_latitude": ("float", "True"),
    "fingerprint_pro_data_ip_v4_longitude": ("float", "True"),
    "device_os": ("string", "True"),
    "downlink": ("string", "True"),
    "effective_type": ("string", "True"),
    "language_code": ("string", "True"),
    "link_persistent_id": ("string", "True"),
    "rtt": ("string", "True"),
    "screen_height": ("string", "True"),
    "screen_width": ("string", "True"),
    "sdk_type": ("string", "True"),
    "sdk_version": ("string", "True"),
    "viewport_height": ("string", "True"),
    "viewport_width": ("string", "True"),
    "fonts": ("string", "True"),
    "domblockers": ("string", "True"),
    "plugin_names": ("string", "True"),
    "distinct_languages": ("string", "True"),
    "vendor_flavors": ("string", "True"),
    "screen_frame": ("string", "True"),
    "screen_resolution": ("string", "True"),
    "hk1": ("string", "True"),
    "hk2": ("string", "True"),
    "hk3": ("string", "True"),
    "hk4": ("string", "True"),
    "c1": ("string", "True"),
    "f1": ("string", "True"),
    "f2": ("string", "True"),
    "n1": ("string", "True"),
    "n2": ("string", "True"),
    "n3": ("string", "True"),
    "n4": ("string", "True"),
    "s1": ("string", "True"),
    "s2": ("string", "True"),
    "s3": ("string", "True"),
    "cpus": ("string", "True"),
    "memory": ("string", "True"),
    "maxwidth": ("string", "True"),
    "timezone": ("string", "True"),
    "unhandled_exceptions": ("string", "True"),
    "security_exceptions": ("string", "True"),
    "integrity": ("string", "True"),
    "battery_level": ("string", "True"),
    "battery_charging": ("string", "True"),
    "battery_charging_time": ("string", "True"),
    "battery_discharging_time": ("string", "True"),
    "device_motion": ("string", "True"),
    "device_orientation": ("string", "True"),
    "vendor": ("string", "True"),
    "link_session_id": ("string", "True"),
    "platform": ("string", "True"),
    "color_gamut": ("string", "True"),
    "video_card_vendor": ("string", "True"),
    "video_card_renderer": ("string", "True"),
    "font_preferences_default": ("double", "True"),
    "font_preferences_apple": ("double", "True"),
    "font_preferences_serif": ("double", "True"),
    "font_preferences_sans": ("double", "True"),
    "font_preferences_mono": ("double", "True"),
    "font_preferences_min": ("double", "True"),
    "profile_id": ("int", "True"),
    "font_preferences_system": ("double", "True"),
    "audio": ("double", "True"),
    "hardware_concurrency": ("int", "True"),
    "touch_support_max_touch_points": ("int", "True"),
    "monochrome": ("int", "True"),
    "contrast": ("int", "True"),
    "architecture": ("int", "True"),
    "webdriver_present": ("boolean", "True"),
    "navigator_stealth_detected": ("boolean", "True"),
    "is_deceptive": ("boolean", "True"),
    "ts": ("timestamp", "True"),
    "session_storage": ("boolean", "True"),
    "local_storage": ("boolean", "True"),
    "indexed_db": ("boolean", "True"),
    "open_database": ("boolean", "True"),
    "touch_support_touch_event": ("boolean", "True"),
    "touch_support_touch_start": ("boolean", "True"),
    "cookies_enabled": ("boolean", "True"),
    "inverted_colors": ("boolean", "True"),
    "forced_colors": ("boolean", "True"),
    "reduced_motion": ("boolean", "True"),
    "dt": ("string", "True"),
    "hdr": ("boolean", "True"),
    "pdf_viewer_enabled": ("boolean", "True"),
    "published_at": ("timestamp", "True"),
    "geoip_autonomous_system_organization": ("string", "True"),
    "geoip_country_code": ("string", "True"),
    "geoip_isp": ("string", "True"),
    "geoip_state_code": ("string", "True"),
    "link_warden_is_matching_link_warden_rule": ("int", "True"),
    "link_warden_link_warden_profile_exists": ("int", "True"),
    "link_warden_navigator_stealth_detected": ("int", "True"),
}

schema_type_map = {
    "int": IntegerType(),
    "boolean": BooleanType(),
    "string": StringType(),
    "double": DoubleType(),
    "float": FloatType(),
    "bigint": LongType(),
    "timestamp": TimestampType(),
}


values_map = {
    "int": random.randint(100000, 999999),
    "boolean": random.choice([True, False]),
    "string": random.choice(["These", "Are", "Random", "Words", "For", "Strings"]),
    "double": random.choice([1.5, 2.0, 3.0]),
    "float": random.uniform(0, 100),
    "bigint": random.randint(10**12, 10**15),
    "timestamp": datetime.datetime.now()
    - datetime.timedelta(days=random.randint(1, 30)),
}


def rand_row(spark_schema):
    vals = [
        (
            values_map[f.dataType.simpleString()]
            if f.name != "dt"
            else random.choice(partition_dates)
        )
        for f in spark_schema.fields
    ]
    return tuple(vals)


spark_schema = StructType(
    [StructField(k, schema_type_map[v[0]], True) for k, v in table_schema.items()]
)
partition_dates = [
    (datetime.datetime.today() - datetime.timedelta(days=i)).strftime("%Y%m%d")
    for i in range(5)
]

data = [rand_row(spark_schema) for _ in range(100)]

spark = SparkSession.builder.appName("plaid_dataset").getOrCreate()
df = spark.createDataFrame(data, schema=spark_schema)


# Show the data
df.select("dt").show(10, truncate=False)


hudi_options = {
    "hoodie.table.name": "plaid_raw",
    "hoodie.datasource.write.partitionpath.field": "dt",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.database.name": "data",
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "data",
    "hoodie.datasource.hive_sync.table": "plaid_raw",
    "hoodie.datasource.hive_sync.partition_fields": "dt",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
}

df.write.format("org.apache.hudi").options(**hudi_options).mode(
    "overwrite"
).partitionBy("dt").save("s3://zipline-warehouse-canary/data/plaid_raw")


# Optionally run this to refresh the catalog partition information.g
# spark.sql("MSCK REPAIR TABLE plaid_raw")

# Optionally run this to check the data
# spark.read \
#     .format("hudi") \
#     .load("s3://zipline-warehouse-canary/data/plaid_raw")
