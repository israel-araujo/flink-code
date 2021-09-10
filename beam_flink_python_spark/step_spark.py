from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime

# Get spark session

spark = SparkSession.builder.appName('convert-files').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
spark.conf.set('spark.sql.debug.maxToStringFields', 100000)

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



def src_android():
    transient = "data-transient-appsflyer-android/data-android"
    date_time = datetime.now() - timedelta(days=1)
    year = date_time.strftime('%Y')
    month = date_time.strftime('%m')
    day = date_time.strftime('%d')
    android_source = spark.read.schema(schema)\
    .json("s3a://{}/{}/{}/{}".format(transient, year, month, day)).cache()
    raw_zone = "data-raw-zone/appsflyer-android"
    android_source.repartition(1).write.mode('append')\
    .mode('overwrite')\
    .option("compression", "gzip")\
    .json("s3a://{}/{}/{}/{}".format(raw_zone, year, month, day))


if __name__ == "__main__":
    src_android()





def src_android():
    transient = "data-transient-appsflyer-android/data-android"
    date_time = datetime.now()
    year = date_time.strftime('%Y')
    month = date_time.strftime('%m')
    day = date_time.strftime('%d')
    android_source = spark.read.schema(schema)\
    .json("s3a://{}/{}/{}/{}".format(transient, year, month, day)).cache()
    raw_zone = "data-raw-zone/appsflyer-android"
    android_source.repartition(1).write.mode('append')\
    .mode('overwrite')\
    .option("compression", "gzip")\
    .json("s3a://{}/{}/{}/{}".format(raw_zone, year, month, day))


if __name__ == "__main__":
    src_android()




from pyspark.sql.types import *

path = '/home/isaraujo@raiadrogasil.com/Downloads/canal_venda2-00000-of-00001.parquet'

df = spark.read.parquet(path)



pathD = ('/home/israel/Downloads/20210716153429_1_A_RAIABD-TB_NF_ITEM_DESCONTO.json')


path = '/home/israel/Documents/beam/modulos/output/israel-00000-of-00001.parquet'


df = spark.read.json(pathD)

path = '/home/israel/Documents/beam/modulos/output/TB_CANAL+-00000-of-00001.json'

df.select('payload.after.CD_CANAL_VENDA','payload.after.DS_CANAL_VENDA').na.drop().show(20, False)


df_read.select('payload.after.CD_CANAL_VENDA','payload.after.DS_CANAL_VENDA').show(20, False)


df.select('CD_CANAL_VENDA','DS_CANAL_VENDA','dt_process_stage').show(20, False)

df2.select('payload.after.CD_CANAL_VENDA','payload.after.DS_CANAL_VENDA').where('payload.after.CD_CANAL_VENDA==5').show(20, False)




[{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"field":"table"},{"type":"string","optional":true,"field":"op_type"},{"type":"string","optional":true,"field":"op_ts"},{"type":"string","optional":true,"field":"current_ts"},{"type":"string","optional":true,"field":"pos"},{"type":"array","items":{"type":"string","optional":false},"optional":true,"field":"primary_keys"},{"type":"map","keys":{"type":"string","optional":false},"values":{"type":"string","optional":false},"optional":true,"field":"tokens"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"after"}],"optional":false,"name":"A_RAIABD.TB_CANAL_VENDA"},"payload":{"table":"A_RAIABD.TB_CANAL_VENDA","op_type":"U","op_ts":"2021-06-17 17:31:31.689976","current_ts":"2021-06-17 17:31:37.786000","pos":"00000000050224513175","primary_keys":["CD_CANAL_VENDA","DS_CANAL_VENDA"],"tokens":{"txid":"0.442.2.1856248"},"before":{"CD_CANAL_VENDA":3,"DS_CANAL_VENDA":"Internet"},"after":{"CD_CANAL_VENDA":3,"DS_CANAL_VENDA":"Internet"}}},




{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"field":"table"},{"type":"string","optional":true,"field":"op_type"},{"type":"string","optional":true,"field":"op_ts"},{"type":"string","optional":true,"field":"current_ts"},{"type":"string","optional":true,"field":"pos"},{"type":"array","items":{"type":"string","optional":false},"optional":true,"field":"primary_keys"},{"type":"map","keys":{"type":"string","optional":false},"values":{"type":"string","optional":false},"optional":true,"field":"tokens"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"after"}],"optional":false,"name":"A_RAIABD.TB_CANAL_VENDA"},"payload":{"table":"A_RAIABD.TB_CANAL_VENDA","op_type":"U","op_ts":"2021-06-17 17:31:31.689976","current_ts":"2021-06-17 17:31:37.785001","pos":"00000000050224513002","primary_keys":["CD_CANAL_VENDA","DS_CANAL_VENDA"],"tokens":{"txid":"0.442.2.1856248"},"before":{"CD_CANAL_VENDA":2,"DS_CANAL_VENDA":"Televendas"},"after":{"CD_CANAL_VENDA":2,"DS_CANAL_VENDA":"Televendas"}}}]