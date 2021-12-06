import pyspark
sc = pyspark.SparkContext()
from pyspark.sql import SparkSession
import csv
from statistics import pstdev, median
from pyspark.sql.functions import year, to_date, stddev, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

def main(sc):

    spark = SparkSession(sc)

    core_place_data = spark.read.csv('core-places-nyc.csv', header=True, escape='"')


    core_place_data = core_place_data.select('placekey', 'naics_code')


    Drinking_place_datafr = core_place_data.filter('naics_code == 722410')


    Conv_stores = core_place_data.filter('naics_code == 445120')

    big_box_grocer = core_place_data.filter('naics_code == 452210' or 'naics_code == 452311')

    Full_service_rest = core_place_data.filter('naics_code == 722511')

    limited_service_rest = core_place_data.filter('naics_code == 722513')

    Pharmacies_drug = core_place_data.filter('naics_code == 446110' or 'naics_code == 446191')

    Snack_bakery = core_place_data.filter('naics_code == 311811' or 'naics_code == 722515')

    Special_food_stores = core_place_data.filter('naics_code == 445210' or 'naics_code == 445220' or 'naics_code == 445230' or 'naics_code == 445291' or 'naics_code == 445292' 'naics_code == 445299')

    Supermarkets_except_convi = core_place_data.filter('naics_code == 445110')

    weekly_pat = spark.read.csv('/content/weekly-patterns-nyc-2019-2020/part-00000', header=True, escape='"')

    weekly_pat = weekly_pat.select('placekey', 'date_range_start', 'visits_by_day')

    visits_by_day = weekly_pat.select('visits_by_day').rdd.map(lambda row: row[0]).collect()
    standard_dev_and_median = []
    for i in range((len(visits_by_day))):
        standard_dev_and_median.append(
            (int(statistics.pstdev(list(map(int, visits_by_day[i].replace('[', '').replace(']', '').split(','))))),
             int(statistics.median(list(map(int, visits_by_day[i].replace('[', '').replace(']', '').split(',')))))))

    standev_median_dataf = spark.createDataFrame(standard_dev_and_median, schema=['stdv', 'median'])
    standev_median_dataf.show()
    w = Window.orderBy(monotonically_increasing_id())

    weekly_pat = weekly_pat.withColumn('year',
                                       year(weekly_pat.date_range_start))  # make coluumns with year date high and low
    weekly_pat = weekly_pat.withColumn('date', to_date(weekly_pat.date_range_start))
    standev_median_dataf = standev_median_dataf.withColumn("index", row_number().over(w))  # necessary for joining
    weekly_pat = weekly_pat.withColumn("index", row_number().over(w))
    weekly_pat = weekly_pat.join(standev_median_dataf, weekly_pat.index == standev_median_dataf.index,
                                 'inner')  # join the two dataframes that have been created to get the columns needed

    weekly_pat = weekly_pat.withColumn('high', (weekly_pat.median + weekly_pat.stdv))
    weekly_pat = weekly_pat.withColumn('low', (weekly_pat.median - weekly_pat.stdv))
    weekly_pat = weekly_pat.drop('index')
    weekly_pat = weekly_pat.drop('stdv')
    weekly_pat = weekly_pat.drop('visits_by_day')  # drop the unneccesary columns needed for the graph
    weekly_pat = weekly_pat.drop('date_range_start')

    big_box_weekly_pat = weekly_pat.join(big_box_grocer, ['placekey'], 'leftsemi')

    Convenience_weekly_pat = weekly_pat.join(Conv_stores, ['placekey'], 'leftsemi')

    Drinking_place_weekly_pat = weekly_pat.join(Drinking_place_datafr, ['placekey'], 'leftsemi')

    Full_serv_weekly_pat = weekly_pat.join(Full_service_rest, ['placekey'], 'leftsemi')

    limited_weekly_pat = weekly_pat.join(limited_service_rest, ['placekey'], 'leftsemi')

    pharmacies_weekly_pat = weekly_pat.join(Pharmacies_drug, ['placekey'], 'leftsemi')

    Snacks_weekly_pat = weekly_pat.join(Snack_bakery, ['placekey'], 'leftsemi')

    Special_weekly_pat = weekly_pat.join(Special_food_stores, ['placekey'], 'leftsemi')

    Supermarkets_noconvi_weekly_pat = weekly_pat.join(Supermarkets_except_convi, ['placekey'], 'leftsemi')

    big_box_weekly_pat.write.csv('big_box_grocers.csv')
    Convenience_weekly_pat.write.csv('convience_store.csv')
    Drinking_place_weekly_pat.write.csv('drinking_places.csv')
    Full_serv_weekly_pat.write.csv('full_service_resturants.csv')
    limited_weekly_pat.write.csv('limited_service_resturants.csv')
    pharmacies_weekly_pat.write.csv('pharmacies_and_drug_stores.csv')
    Snacks_weekly_pat.write.csv('snack_and_bakeries.csv')
    Special_weekly_pat.write.csv('specialty_food_stores.csv')
    Supermarkets_noconvi_weekly_pat.write.csv('supermakets_except_convenience_stores.csv')

 if __name__ == "__main__":
    sc = pyspark.SparkContext()
    main(sc)



