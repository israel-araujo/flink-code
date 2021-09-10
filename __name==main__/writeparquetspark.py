from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Get spark session
spark = SparkSession.builder.appName('ios_processed_zone').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
spark.conf.set('spark.sql.debug.maxToStringFields', 1000000000)

schema = StructType([
         StructField('advertising_id', StringType()),
         StructField('af_ad', StringType()),
         StructField('af_ad_id', StringType()),     
         StructField('af_ad_type', StringType()),
         StructField('af_adset', StringType()),
         StructField('af_adset_id', StringType()),
         StructField('af_attribution_lookback', StringType()),
         StructField('af_c_id', StringType()),
         StructField('af_channel', StringType()),
         StructField('af_cost_currency', StringType()),
         StructField('af_cost_model', StringType()),
         StructField('af_cost_value', StringType()),
         StructField('af_keywords', StringType()),
         StructField('af_prt', StringType()),
         StructField('af_reengagement_window', StringType()),
         StructField('af_siteid', StringType()),
         StructField('af_sub1', StringType()),
         StructField('af_sub2', StringType()),
         StructField('af_sub3', StringType()),
         StructField('af_sub4', StringType()),
         StructField('af_sub5', StringType()),
         StructField('af_sub_siteid', StringType()),
         StructField('amazon_aid', StringType()),
         StructField('android_id', StringType()),
         StructField('api_version', StringType()),
         StructField('app_id', StringType()),
         StructField('app_name', StringType()),
         StructField('app_version', StringType()),
         StructField('appsflyer_id', StringType()),
         StructField('attributed_touch_time', StringType()),
         StructField('attributed_touch_time_selected_timezone', StringType()),
         StructField('attributed_touch_type', StringType()),
         StructField('bundle_id', StringType()),
         StructField('campaign', StringType()),
         StructField('carrier', StringType()),
         StructField('city', StringType()),
         StructField('contributor_1_af_prt', StringType()),
         StructField('contributor_1_campaign', StringType()),
         StructField('contributor_1_match_type', StringType()),
         StructField('contributor_1_media_source', StringType()),
         StructField('contributor_1_touch_time', StringType()),
         StructField('contributor_1_touch_type', StringType()),
         StructField('contributor_2_af_prt', StringType()),
         StructField('contributor_2_campaign', StringType()),
         StructField('contributor_2_match_type', StringType()),
         StructField('contributor_2_media_source', StringType()),
         StructField('contributor_2_touch_time', StringType()),
         StructField('contributor_2_touch_type', StringType()),
         StructField('contributor_3_af_prt', StringType()),
         StructField('contributor_3_campaign', StringType()),
         StructField('contributor_3_match_type', StringType()),
         StructField('contributor_3_media_source', StringType()),
         StructField('contributor_3_touch_time', StringType()),
         StructField('contributor_3_touch_type', StringType()),
         StructField('cost_in_selected_currency', StringType()),
         StructField('country_code', StringType()),
         StructField('custom_data', StringType()),
         StructField('customer_user_id', StringType()),
         StructField('deeplink_url', StringType()),
         StructField('device_category', StringType()),
         StructField('device_download_time', StringType()),
         StructField('device_download_time_selected_timezone', StringType()),
         StructField('device_type', StringType()),
         StructField('dma', StringType()),
         StructField('download_time', StringType()),
         StructField('download_time_selected_timezone', StringType()),
         StructField('event_name', StringType()),
         StructField('event_revenue', StringType()),
         StructField('event_revenue_currency', StringType()),
         StructField('event_revenue_usd', StringType()),
         StructField('event_source', StringType()),
         StructField('event_time', StringType()),
         StructField('event_time_selected_timezone', StringType()),
         StructField('event_value', StringType()),
         StructField('gp_broadcast_referrer', StringType()),
         StructField('gp_click_time', StringType()),
         StructField('gp_install_begin', StringType()),
         StructField('gp_referrer', StringType()),
         StructField('http_referrer', StringType()),
         StructField('idfa', StringType()),
         StructField('idfv', StringType()),
         StructField('imei', StringType()),
         StructField('install_app_store', StringType()),
         StructField('install_time', StringType()),
         StructField('install_time_selected_timezone', StringType()),
         StructField('ip', StringType()),
         StructField('is_lat', StringType()),
         StructField('is_primary_attribution', BooleanType()),
         StructField('is_receipt_validated', StringType()),
         StructField('is_retargeting', BooleanType()),
         StructField('keyword_id', StringType()),
         StructField('keyword_match_type', StringType()),
         StructField('language', StringType()),
         StructField('match_type', StringType()),
         StructField('media_source', StringType()),
         StructField('network_account_id', StringType()),
         StructField('oaid', StringType()),
         StructField('operator', StringType()),
         StructField('original_url', StringType()),
         StructField('os_version', StringType()),
         StructField('platform', StringType()),
         StructField('postal_code', StringType()),
         StructField('region', StringType()),
         StructField('retargeting_conversion_type', StringType()),
         StructField('revenue_in_selected_currency', StringType()),
         StructField('sdk_version', StringType()),
         StructField('selected_currency', StringType()),
         StructField('selected_timezone', StringType()),
         StructField('state', StringType()),
         StructField('store_reinstall', StringType()),
         StructField('user_agent', StringType()),
         StructField('wifi', BooleanType()),
])


def android_parquet():
    raw_zone = "data-raw-zone/appsflyer-android"
    date_time = datetime.now() - timedelta(days=1)
    year = date_time.strftime('%Y')
    month = date_time.strftime('%m')
    day = date_time.strftime('%d')
    dst_new_fmt_android = spark.read.schema(schema)\
    .json('s3a://{}/{}/{}/{}/'.format(raw_zone, year, month, day))
    dst_new_fmt_android_clear = dst_new_fmt_android.distinct().cache()
    processed_zone = ('data-processed-zone/android-fmt-parquet')
    dst_new_fmt_android_clear\
    .repartition(1).write.mode('append').mode('overwrite')\
    .parquet('s3a://{}/{}/{}/{}'.format(processed_zone, year, month, day))


if __name__ == "__main__":
    android_parquet()





from pyspark.sql import *

def WriteToParquet():
    path  = '/home/israel/Documents/beam/modulos/input/part-00000-50fc0dbd-4d0d-4dac-b7bc-f0cbda089b2e.c000.snappy.parquet'
    df_read = spark.read.parquet(path)
    df_write = df_read.repartition(1).write.mode('append').parquet('/home/israel/Documents/beam/modulos/output')

if __name__ == "__main__":


from pyspark.sql import *


path  = '/home/isaraujo@raiadrogasil.com/Downloads/part-00000-c122e72f-e591-4623-9fcc-c5daaa5aaa64.c000.snappy.parquet'
df_read = spark.read.parquet(path)
df_write = df_read.repartition(1).write.mode('append').parquet('/home/isaraujo@raiadrogasil.com/Documentos/flink-code/output/')




for linha in path(linha =+ path): print