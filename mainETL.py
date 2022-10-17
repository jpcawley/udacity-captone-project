import configparser
import datetime
import os
import time
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import *
from functools import reduce
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType as R, StructField as Fld


def get_S3_bucket():
    ''' Retrieve S3 bucket path. '''

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    BUCKET = config.get('S3', 'BUCKET')

    return BUCKET


def create_spark_session():
    ''' Function that creates or retrieves an existing SparkSession. '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def saffir_simpson_wind_scale(spark, bucket):
    '''
    Function that creates the saffir_simpson_wind_scale table.
    source: https://www.nhc.noaa.gov/aboutsshws.php
    '''
    data = [{'category': '1', 'min_sustained_wind_kt': 64, 'max_sustained_wind_kt': 82, 
             'brief_damage_description': 'Power outages that could last a few to several days.'}, 
            {'category': '2', 'min_sustained_wind_kt': 83, 'max_sustained_wind_kt': 95, 
             'brief_damage_description': 'Near-total power loss is expected with outages that could last from several days to weeks.'},
            {'category': '3-MAJOR', 'min_sustained_wind_kt': 96, 'max_sustained_wind_kt': 112, 
             'brief_damage_description': 'Electricity and water will be unavailable for several days to weeks after the storm passes.'},
            {'category': '4-MAJOR', 'min_sustained_wind_kt': 113, 'max_sustained_wind_kt': 136, 
             'brief_damage_description': 'Catastrophic damage will occur; most of the area will be uninhabitable for weeks or months.'},
            {'category': '5-MAJOR', 'min_sustained_wind_kt': 137, 'max_sustained_wind_kt': 1000000, 
             'brief_damage_description': 'Catastrophic damage will occur; most of the area will be uninhabitable for weeks or months.'}
           ]
    
    pd_wind_scale = pd.DataFrame(data)
    
    spark_wind_scale = spark.createDataFrame(data)

    saffir_simpson_hurricane_wind_scale = spark_wind_scale.select('category', 'min_sustained_wind_kt', 
                                                                  'max_sustained_wind_kt', 'brief_damage_description')
    
    wind_scale = pd_wind_scale[['category', 'min_sustained_wind_kt', 'max_sustained_wind_kt', 'brief_damage_description']]
    
    saffir_simpson_hurricane_wind_scale.write.mode('overwrite').parquet(os.path.join(bucket, 'transformed_data/dim_tables/wind_scale.parquet'))
    
    return wind_scale, saffir_simpson_hurricane_wind_scale

def process_name_data(spark, bucket):
    '''
    Function that processes name data using PySpark.
    Reads name data from S3, transforms it, and writes it back to s3 as parquet.

    Parameters:
        spark: the SparkSession
        bucket: S3 bucket to read from and write to
    '''

    raw_name_data = spark.read.text(
        os.path.join(bucket, '*', '*', '*.TXT'))

    split_col = F.split(raw_name_data.value, ',')
    names_by_state = raw_name_data.withColumn('state_code', split_col.getItem(0)) \
    .withColumn('sex', split_col.getItem(1)) \
    .withColumn('birth_year', split_col.getItem(2)) \
    .withColumn('birth_name', split_col.getItem(3)) \
    .withColumn('count', split_col.getItem(4)) \
    .drop(raw_name_data.value)

    names_by_state = names_by_state.orderBy(['birth_year', 'state_code', 'birth_name']) \
    .withColumn('name_id', F.row_number().over(Window.partitionBy().orderBy('birth_year', 'state_code', 'birth_name'))) \
    .withColumn('decade', (F.floor(F.col('birth_year')/10)*10).cast('int')) \
    .withColumn('birth_name', F.upper(F.col('birth_name')))

    baby_names_by_state = names_by_state.select('name_id', 'birth_name', 'birth_year', 'state_code', 'sex', 'count', 'decade')

    names_by_state.write.partitionBy('state_code', 'birth_year').mode(
        'overwrite').parquet(os.path.join(bucket, 'transformed_data/dim_tables/names_by_state.parquet'))

    return names_by_state


def process_state_data(spark, bucket):
    '''
    Function that processes name data using PySpark.
    Reads state data from S3, transforms it, and writes it back to s3 as parquet.

    Parameters:
        spark: the SparkSession
        bucket: S3 bucket to read from and write to
    '''

    raw_state_data = spark.read.json(os.path.join(bucket, '*', 'states.json'))

    state_data = raw_state_data.withColumnRenamed('State', 'state') \
    .withColumnRenamed('State Code', 'state_code') \
    .withColumnRenamed('Region', 'region') \
    .withColumnRenamed('Division', 'division') \
    .withColumn('state_id', F.row_number().over(Window.partitionBy().orderBy('state', 'state_code')))

    state_data = state_data.select('state_id', 'state_code', 'state', 'region', 'division')

    # create us_states_by_region table
    state_data.write.mode('overwrite').parquet(
        os.path.join(bucket, 'transformed_data/dim_tables/state_data.parquet'))

    return state_data

def define_schemas():
    '''
    Create schemas for storms stats and combined storms data tables.
    '''
    
    storm_stats_schema = StructType([
        StructField('index', StringType()),
        StructField('storm_date', DateType()),
        StructField('storm_year', IntegerType()),
        StructField('storm_month', IntegerType()),
        StructField('storm_day', IntegerType()),
        StructField('storm_time', StringType()),
        StructField('record_identifier', StringType()),
        StructField('storm_status', StringType()),
        StructField('category', StringType()),
        StructField('latitude', DoubleType()),
        StructField('longitude', DoubleType()),
        StructField('max_sustained_wind_kt', IntegerType()),
        StructField('min_pressure(mbar)', IntegerType())
    ])

    storm_schema = R([
        Fld('storm_id', StringType()),
        Fld('basin', StringType()),
        Fld('atcf_cyclone_num', IntegerType()),
        Fld('storm_name', StringType()),
        Fld('storm_date', DateType()),
        Fld('storm_year', IntegerType()),
        Fld('storm_month', IntegerType()),
        Fld('storm_day', IntegerType()),
        Fld('storm_time', StringType()),
        Fld('record_identifier', StringType()),
        Fld('storm_status', StringType()),
        Fld('storm_category', StringType()),
        Fld('latitude', DoubleType()),
        Fld('longitude', DoubleType()),
        Fld('max_sustained_wind_kt', IntegerType()),
        Fld('min_pressure(mbar)', IntegerType())
    ])
    
    return storm_stats_schema, storm_schema

def read_storm_data(spark, bucket):
    '''
    Function that reads raw storm data from S3.

    Parameters:
        spark: the SparkSession
        bucket: S3 bucket to read from and write to
    '''
    raw_storm_data = spark.read.csv(os.path.join(
        bucket, '*', 'raw_storm_data.csv'), header=True)
    
    return raw_storm_data

def process_storms_headers(bucket, raw_storm_data):
    '''
    Function that processes the headers from the storms data using PySpark.

    Parameters:
        bucket: S3 bucket to read from and write to
        raw_storm_data: spark dataframe with raw storm data
    '''

    identified_storms = raw_storm_data.filter(raw_storm_data['0'].contains('AL')).withColumnRenamed('0', 'storm_id') \
    .withColumnRenamed('1', 'storm_name').withColumnRenamed('2', 'entries') \
    .drop('3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20')

    atlantic_storms_by_year = identified_storms.withColumn('storm_id', F.trim(identified_storms.storm_id)) \
    .withColumn('storm_name', F.trim(F.upper(F.col('storm_name')))) \
    .withColumn('entries', F.trim(identified_storms.entries).cast(IntegerType())) \
    .withColumn('basin', F.substring(F.col('storm_id'), 1, 2)) \
    .withColumn('atcf_cyclone_num', F.substring(F.col('storm_id'), 3, 2).cast(IntegerType())) \
    .withColumn('storm_year', F.substring(F.col('storm_id'), 5, 8).cast(IntegerType())) \
    .withColumn('header_id', identified_storms._c0.cast(IntegerType())) \
    .drop('_c0') \
    .dropDuplicates()

    atlantic_storms_header = atlantic_storms_by_year.withColumn('header_id', atlantic_storms_by_year['header_id'].cast(IntegerType())).orderBy(F.asc('header_id')) \
    .select('header_id', 'storm_id', 'storm_name', 'entries', 'basin', 'atcf_cyclone_num', 'storm_year')

    atlantic_storms_header.write.mode('overwrite').parquet(os.path.join(bucket, 'transformed_data/dim_tables/storm_data/storm_headers.parquet'))

    return atlantic_storms_header

def process_storms_stats(spark, bucket, raw_storm_data, stats_schema):
    '''
    Function that processes the statistics from the storms data using PySpark.

    Parameters:
        spark: the SparkSession
        bucket: S3 bucket to read from and write to
        raw_storm_data: spark dataframe with raw storm data
        stats_schema: schema for storms statistics table
    '''
    
    spark_storm_stats = raw_storm_data.filter(~raw_storm_data['0'].contains('AL')) \
    .withColumnRenamed('0', 'date') \
    .withColumnRenamed('1', 'time') \
    .withColumnRenamed('2', 'record_identifier') \
    .withColumnRenamed('3', 'storm_status') \
    .withColumnRenamed('4', 'latitude') \
    .withColumnRenamed('5', 'longitude') \
    .withColumnRenamed('6', 'max_sustained_wind_kt')\
    .withColumnRenamed('7', 'min_pressure(mbar)') \
    .drop('8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20')

    spark_storm_stats = spark_storm_stats.withColumn('storm_date', F.trim(spark_storm_stats.date)) \
    .withColumn('storm_time', F.trim(spark_storm_stats.time)) \
    .withColumn('record_identifier', F.trim(spark_storm_stats.record_identifier)) \
    .withColumn('storm_status', F.trim(spark_storm_stats.storm_status)) \
    .withColumn('max_sustained_wind_kt', F.trim(spark_storm_stats['max_sustained_wind_kt']).cast(IntegerType()))\
    .withColumn('min_pressure(mbar)', F.trim(spark_storm_stats['min_pressure(mbar)']).cast(IntegerType())) \
    .withColumn('stat_id', spark_storm_stats._c0.cast(IntegerType()))

    # extract year, month, and day from date column
    spark_storm_stats = spark_storm_stats.withColumn('storm_date', F.to_date(F.col('date'),'yyyyMMdd') )
    spark_storm_stats = spark_storm_stats.withColumn('storm_year', F.year(spark_storm_stats.storm_date).cast(IntegerType())) \
    .withColumn('storm_month', F.month(spark_storm_stats.storm_date).cast(IntegerType())) \
    .withColumn('storm_day', F.dayofmonth(spark_storm_stats.storm_date).cast(IntegerType())) 

    spark_storm_stats = spark_storm_stats.select('stat_id', 'storm_date', 'storm_year', 'storm_month', 'storm_day', 'storm_time', 'record_identifier', 
                               'storm_status', 'latitude', 'longitude', 'max_sustained_wind_kt', 'min_pressure(mbar)')

    atlantic_storm_stats = spark_storm_stats.toPandas()

    # doesn't format correctly in spark
    # atlantic_storm_stats['storm_time'] = pd.to_datetime(
    #         atlantic_storm_stats['storm_time'].str.strip(), format='%H%M').dt.time

    # tranform storm_status and record_identifier in atlantic_storms_stats
    atlantic_storm_stats.storm_status = atlantic_storm_stats.storm_status.str.strip() \
    .map({'HU': 'hurricane', 'TS': 'tropical storm', 'EX': 'extratropical cyclone',
         'TD': 'tropical depression', 'LO': 'low pressure system', 'DB': 'disturbance', 
          'SD': 'subtropical depression', 'SS': 'subtropical storm', 'WV': 'tropical wave'})

    atlantic_storm_stats.record_identifier = atlantic_storm_stats.record_identifier.str.strip()  \
    .map({'': '', 'L': 'landfall', 'R': 'intensity details with rapid changes',
          'I': 'pressure and wind intensity peak', 'P': 'min central pressure',
          'T': 'clarify track detail', 'W': 'max sustained wind speed',
          'C': 'approach to coast, no landfall', 'S': 'status change in system',
          'G': 'genesis of the system'})

    # transform lat and long
    lat_north = pd.to_numeric(atlantic_storm_stats['latitude'].str[:-1])
    lat_south = pd.to_numeric(atlantic_storm_stats['latitude'].str[:-1])*-1
    long_east = pd.to_numeric(atlantic_storm_stats['longitude'].str[:-1])
    long_west = pd.to_numeric(atlantic_storm_stats['longitude'].str[:-1])*-1

    atlantic_storm_stats['latitude'] = np.where(
        atlantic_storm_stats['latitude'].str[-1:] == 'N', lat_north, lat_south)
    atlantic_storm_stats['longitude'] = np.where(
        atlantic_storm_stats['longitude'].str[-1:] == 'E', long_east, long_west)

    # get hurricane category by sustained_wind(kt)

    atlantic_storm_stats['storm_category'] = np.where((atlantic_storm_stats['max_sustained_wind_kt'] < wind_scale['min_sustained_wind_kt'][0]), 
                                                      atlantic_storm_stats['storm_status'], 'uncategorized')
    atlantic_storm_stats['storm_category'] = np.where((atlantic_storm_stats['max_sustained_wind_kt'].between(wind_scale['min_sustained_wind_kt'][0], wind_scale['max_sustained_wind_kt'][0]+1)), 
                                                      wind_scale['category'][0], atlantic_storm_stats['storm_category'])
    atlantic_storm_stats['storm_category'] = np.where((atlantic_storm_stats['max_sustained_wind_kt'].between(wind_scale['min_sustained_wind_kt'][1], wind_scale['max_sustained_wind_kt'][1]+1)), 
                                                      wind_scale['category'][1], atlantic_storm_stats['storm_category'])
    atlantic_storm_stats['storm_category'] = np.where((atlantic_storm_stats['max_sustained_wind_kt'].between(wind_scale['min_sustained_wind_kt'][2], wind_scale['max_sustained_wind_kt'][2]+1)), 
                                                      wind_scale['category'][2], atlantic_storm_stats['storm_category'])
    atlantic_storm_stats['storm_category'] = np.where((atlantic_storm_stats['max_sustained_wind_kt'].between(wind_scale['min_sustained_wind_kt'][3], wind_scale['max_sustained_wind_kt'][3]+1)), 
                                                      wind_scale['category'][3], atlantic_storm_stats['storm_category'])
    atlantic_storm_stats['storm_category'] = np.where((atlantic_storm_stats['max_sustained_wind_kt'] >= wind_scale['min_sustained_wind_kt'][4]), 
                                                      wind_scale['category'][4], atlantic_storm_stats['storm_category'])

    pd_storm_stats = atlantic_storm_stats[['stat_id', 'storm_date', 'storm_year', 'storm_month', 'storm_day', 'storm_time', 
          'record_identifier', 'storm_status', 'storm_category', 'latitude', 'longitude', 'max_sustained_wind_kt', 'min_pressure(mbar)']]

    atlantic_storm_stats = spark.createDataFrame(pd_storm_stats, stats_schema)
    
    atlantic_storm_stats.write.mode('overwrite').parquet(os.path.join(bucket, 'transformed_data/dim_tables/storm_data/storm_stats.parquet'))
    
    return atlantic_storm_stats

def process_atlantic_storms_data(spark, bucket, storms_headers, storms_stats):
    '''
    Function that processes name data using PySpark.
    Reads name data from S3, cleans the data, and writes it back to s3 as json.

    Parameters:
        bucket: S3 bucket to write data to
        storms_headers: Dataframe with storm header information
        storms_stats: Dataframe with storm statistics
    '''

    data = []

    storms_by_year = atlantic_storms_header.toPandas()

    storm_ids = list(storms_by_year.storm_id)
    names = list(storms_by_year.storm_name)
    basins = list(storms_by_year.basin)
    cyclone_nums = list(storms_by_year.atcf_cyclone_num)
    entries = storms_by_year.entries.apply(lambda x: int(x)).tolist()

    cols = ['storm_id', 'storm_name', 'basin', 'atcf_cyclone_num', 'storm_date', 'storm_year', 'storm_time', 'record_identifier',
        'storm_status', 'storm_category', 'latitude', 'longitude', 'max_sustained_wind_kt', 'min_pressure(mbar)']

    for i, nrows, nxt in zip(range(len(storms_by_year['header_id'])), entries, storms_by_year['header_id']):
        temp_df = pd.DataFrame(pd_storm_stats.loc[(pd_storm_stats['stat_id'] >= nxt) & (pd_storm_stats['stat_id'] <= (nxt + nrows))])
        temp_df['storm_id'] = storm_ids[i]
        temp_df['storm_name'] = names[i]
        temp_df['basin'] = basins[i]
        temp_df['atcf_cyclone_num'] = cyclone_nums[i]
        temp_df = temp_df[cols]
        data.append(temp_df)

    pd_atlantic_storms = pd.concat(
            data).reset_index(drop=True)[['storm_id', 'basin', 'atcf_cyclone_num', 'storm_name', 'storm_date', 'storm_year', 'storm_time', 'record_identifier',
            'storm_status', 'storm_category', 'latitude', 'longitude', 'max_sustained_wind_kt', 'min_pressure(mbar)']]
    
    atlantic_storms = spark.createDataFrame(pd_atlantic_storms)

    atlantic_storms = atlantic_storms.withColumn('atl_id', F.row_number().over(Window.partitionBy().orderBy('storm_id'))) \
    .select('atl_id', 'storm_id', 'basin', 'atcf_cyclone_num', 'storm_name', 'storm_date', 'storm_year', 'storm_time', 'record_identifier',
        'storm_status', 'storm_category', 'latitude', 'longitude', 'max_sustained_wind_kt', 'min_pressure(mbar)')
    
    atlantic_storms.write.mode('overwrite').parquet(os.path.join(bucket, 'transformed_data/dim_tables/storm_data/atlantic_storms.parquet'))

    return atlantic_storms

def named_atlantic_storms_locations(spark, bucket, atlantic_storms, state_data):
    '''
    Function that processes the atlantic storms data to add state information based on latitude and longitude.

    Parameters:
        spark: the SparkSession
        bucket: S3 bucket to read from and write to
        raw_storm_data: spark dataframe with raw storm data
    '''

    import ssl
    import certifi
    import geopy.geocoders
    from geopy.geocoders import Nominatim

    geopy.geocoders.options.default_ssl_context=ssl.create_default_context(cafile=certifi.where())

    geolocator=Nominatim(user_agent="capstone", scheme='http', timeout=None)

    pd_atlantic_storms = atlantic_storms.toPandas()

    pd_state_data = state_data.toPandas()

    named_atlantic_storms=pd_atlantic_storms.loc[~pd_atlantic_storms['storm_name'].str.contains(
        'UNNAMED')]
    records=named_atlantic_storms.loc[named_atlantic_storms['record_identifier'] == 'landfall'].reset_index(
        drop=True)

    states_with_codes=dict(
        zip(pd_state_data['state'], pd_state_data['state_code']))

    location_data=[]
    no_state=0

    for row in records.itertuples(index=False):
        dct={}
        coordinates=" ' " + row.latitude + ', ' + row.longitude + " ' " 
        location=geolocator.reverse(coordinates)
#         location=geolocator.reverse(f'{row.latitude}, {row.longitude}')
        if location is not None:
            storm_state=location.raw.get('address').get('state')
            if storm_state not in states_with_codes.keys():
                no_state+=1
                continue
            else:
                dct['atl_id']=row.atl_id
                dct['storm_id']=row.storm_id
                dct['storm_name']=row.storm_name
                dct['storm_year']=row.storm_year
                dct['storm_status']=row.storm_status
                dct['storm_category']=row.storm_category
                dct['state']=storm_state
                dct['latitude']=row.latitude
                dct['longitude']=row.longitude
                temp_df=pd.DataFrame([dct], columns=['storm_id', 'storm_name', 'storm_year',
                                     'storm_status', 'storm_category', 'latitude', 'longitude', 'state'])
                location_data.append(temp_df)
        else:
            no_state+=1

    storms_with_landfall=pd.concat(location_data, sort=False).reset_index().drop_duplicates()
    
    swl = spark.createDataFrame(storms_with_landfall)

    swl = swl.withColumn('swl_id', F.row_number().over(Window.partitionBy().orderBy('storm_id', 'storm_name')))

    named_atlantic_storms_with_us_landfall = swl.join(state_data, 'state', 'inner').select(swl.swl_id, swl.storm_id, swl.storm_name, swl.storm_year,
                                     swl.storm_status, swl.storm_category, state_data.state_code)

    named_atlantic_storms_with_us_landfall.write.mode('overwrite').parquet(os.path.join(bucket, 'transformed_data/fact_tables/storms_with_us_landfall.parquet'))

    return named_atlantic_storms_with_us_landfall

def names_by_person_and_storm(names_df, named_storms_df, wind_scale):
    '''
    Function that creates names by person and storm fact table.
    '''

    storms = named_atlantic_storms_with_us_landfall.alias('storms')
    names = names_by_state.alias('names')
    winds_scale = saffir_simpson_hurricane_wind_scale.alias('winds_scale')

    names_by_person_and_storm = storms.join(names, storms.storm_name == names.birth_name) \
    .join(winds_scale, storms.storm_category == winds_scale.category, 'left') \
    .select(storms.swl_id, storms.storm_id, names.name_id, storms.storm_name, names.birth_name.alias('baby_name'), storms.storm_year, 
            names.birth_year.alias('baby_birth_year'), storms.state_code, names.sex.alias('baby_sex'), storms.storm_status, 
            storms.storm_category, winds_scale.brief_damage_description.alias('storm_damage_description'), names['count'].alias('name_count'))

    names_by_person_and_storm = names_by_person_and_storm.withColumn('bsn_id', F.row_number().over(Window.partitionBy().orderBy(names_by_person_and_storm.name_count)))

    names_by_person_and_storm = names_by_person_and_storm.select('bsn_id', 'swl_id', 'storm_id', 'name_id', 'storm_name', 'baby_name', 'storm_year', 'baby_birth_year', 
                                                                 'state_code', 'storm_status', 'storm_category', 'storm_damage_description', 'baby_sex', 'name_count')
    
    names_by_person_and_storm.write.mode('overwrite').parquet(os.path.join(bucket, 'transformed_data/fact_tables/names_by_person_and_storm_fact.parquet'))

    return

def main():
    '''
    Main function to perform ETL.
    '''
    start_time = time.time()

    bucket = get_S3_bucket()
    
    spark = create_spark_session()

    # task parallelism: https://knowledge.udacity.com/questions/73278
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    # install python packages
#     sc.install_pypi_package("pandas==0.23.2")
#     sc.install_pypi_package("geopy==2.2.0")
#     sc.install_pypi_package("certifi==2019.11.28")
    
    wind_scale = saffir_simpson_wind_scale(spark, bucket)
    
    names_dim = process_name_data(spark, bucket)
    
    states_dim = process_state_data(spark, bucket)
    
    stats_schema, storm_schema = define_schemas()
    raw_storm_df = read_storm_data(spark, bucket)
    headers_dim = process_storms_headers(bucket, raw_storm_df)
    stats_dim = process_storms_stats(spark, bucket, raw_storm_df, stats_schema)
    storms_dim = process_atlantic_storms_data(spark, bucket, headers_dim, stats_dim)
    named_storms_dim = named_atlantic_storms_locations(spark, bucket, atlantic_storms, states_dim)
    
    people_and_storm_name_fact = names_by_person_and_storm(names_dim, named_storms_dim, wind_scale)
    
    end_time = time.time()

    elapsed_time = (end_time - start_time) / 60

    print("ETL processing completed in: ~{:.2f} seconds or ~{:.2f} minutes.".format(
        (end_time - start_time), elapsed_time))


if __name__ == "__main__":
    main()
