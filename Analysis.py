from pyspark.sql import Window
from pyspark.sql.functions import *


def analysis_1(spark, primary_person_use):
    print("Answer for Analysis_1 is  :")
    print(spark.read.csv(primary_person_use, header=True).filter("PRSN_GNDR_ID='MALE'").count())


def analysis_2(spark, units_use):
    print("Answer for Analysis_2 is  :")
    print(spark.read.csv(units_use, header=True).where('VEH_BODY_STYL_ID like "%MOTORCYCLE%"').count())


def analysis_3(spark, primary_person_use):
    print("Answer for Analysis_3 is  :")
    df = spark.read.csv(primary_person_use, header=True).filter("PRSN_GNDR_ID='FEMALE'")

    df = df.groupBy('DRVR_LIC_STATE_ID').agg(count('DRVR_LIC_STATE_ID').alias('CNT'))

    df.orderBy(col('CNT').desc()).limit(1).select('DRVR_LIC_STATE_ID').show(truncate=False)


def analysis_4(spark, units_use):
    print("Answer for Analysis_4 is  :")
    window = Window.orderBy(col('TOTAL_INJURIES').desc())
    df = spark.read.csv(units_use, header=True).filter("TOT_INJRY_CNT!=0 or DEATH_CNT!=0")

    df = df.withColumn('TOTAL', ((col('TOT_INJRY_CNT') + col('DEATH_CNT'))))

    df = df.groupBy('VEH_MAKE_ID').agg(sum("TOTAL").alias('TOTAL_INJURIES')).orderBy(col('TOTAL_INJURIES').desc())

    df = df.withColumn("row_number", dense_rank().over(window)).filter("row_number>=5 and row_number<=15")

    df.drop('row_number').select('VEH_MAKE_ID').show(truncate=False)


def analysis_5(spark, units_use, primary_person_use):
    print("Answer for Analysis_5 is  :")
    window = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('CNT').desc())

    units_df = spark.read.csv(units_use, header=True)
    person_df = spark.read.csv(primary_person_use, header=True)

    join_df = units_df.join(person_df, 'CRASH_ID')

    join_df = join_df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').agg(count('PRSN_ETHNICITY_ID').alias('CNT'))

    join_df = join_df.orderBy(col('VEH_BODY_STYL_ID')).withColumn("row_number", dense_rank().over(window))

    join_df.filter("row_number=1").drop('row_number').select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').show(truncate=False)


def analysis_6(spark, primary_person_use, units_use):
    print("Answer for Analysis_6 is  :")
    person_df = spark.read.csv(primary_person_use, header=True)

    units_df = spark.read.csv(units_use, header=True)

    join_df = person_df.join(units_df, "CRASH_ID")

    join_df = join_df.where(
        "CONTRIB_FACTR_1_ID like '%ALCOHOL%' or CONTRIB_FACTR_2_ID like '%ALCOHOL%' or CONTRIB_FACTR_P1_ID like '%ALCOHOL%'")

    join_df = join_df.groupBy('DRVR_ZIP').agg(count('DRVR_ZIP').alias('CNT'))

    join_df.orderBy(col('CNT').desc()).limit(5).select('DRVR_ZIP').show(truncate=False)


def analysis_7(spark, units_use, damages_use):
    print("Answer for Analysis_7 is  :")
    units_df = spark.read.csv(units_use, header=True)

    damages_df = spark.read.csv(damages_use, header=True)

    join_df = damages_df.join(units_df, 'CRASH_ID')

    join_df = join_df.filter("FIN_RESP_TYPE_ID!='NA'")
    join_df = join_df.filter(
        'VEH_DMAG_SCL_1_ID =="DAMAGED 4" or VEH_DMAG_SCL_1_ID =="DAMAGED 5" or VEH_DMAG_SCL_1_ID =="DAMAGED 6" or VEH_DMAG_SCL_1_ID =="DAMAGED 7 HIGHEST" or VEH_DMAG_SCL_2_ID =="DAMAGED 4" or VEH_DMAG_SCL_2_ID =="DAMAGED 5" or VEH_DMAG_SCL_2_ID =="DAMAGED 6" or VEH_DMAG_SCL_2_ID =="DAMAGED 7 HIGHEST"')
    print(join_df.select('CRASH_ID').distinct().count())


def analysis_8(spark, primary_person_use, units_use):
    print("Answer for Analysis_8 is  :")
    person_df = spark.read.csv(primary_person_use, header=True)

    units_df = spark.read.csv(units_use, header=True)

    filter_1 = person_df.groupBy('DRVR_LIC_STATE_ID').agg(count('DRVR_LIC_STATE_ID').alias('CNT')).orderBy(
        col('DRVR_LIC_STATE_ID').desc()).limit(25).select(collect_list('DRVR_LIC_STATE_ID').alias("FILTER_1")).select(
        'FILTER_1').collect()[0][0]

    filter_2 = \
    units_df.groupBy('VEH_COLOR_ID').agg(count('VEH_COLOR_ID').alias('CNT')).orderBy(col('CNT').desc()).limit(
        10).select(collect_list('VEH_COLOR_ID').alias("FILTER_2")).select('FILTER_2').collect()[0][0]

    join_df = person_df.join(units_df, "CRASH_ID")

    join_df = join_df.where('DRVR_LIC_CLS_ID <> "NA" or DRVR_LIC_CLS_ID <>"UNKNOWN" or DRVR_LIC_CLS_ID <>"UNLICENSED"')

    join_df = join_df.where(
        "CONTRIB_FACTR_1_ID like '%SPEED%' or CONTRIB_FACTR_2_ID like '%SPEED%' or CONTRIB_FACTR_P1_ID like '%SPEED%'")

    join_df = join_df.filter(col("DRVR_LIC_STATE_ID").isin(filter_1))

    join_df = join_df.filter(col("VEH_COLOR_ID").isin(filter_2))

    join_df.groupBy('VEH_MAKE_ID').agg(count('VEH_MAKE_ID').alias('CNT')).orderBy(col('CNT').desc()).select(
        'VEH_MAKE_ID').limit(5).show(truncate=False)