from pyspark.sql import SparkSession
from pyspark.sql.functions import count as _count, col, when, row_number, desc, expr
from pyspark.sql.window import Window
from utils import read_csv

class SparkCrashAnalysis:
    def __init__(self, spark: SparkSession):
        """
        Initialize with a SparkSession and load datasets.
        """
        self.spark = spark
        self.charges_df = read_csv(spark, './data/Charges_use.csv')
        self.damages_df = read_csv(spark, './data/Damages_use.csv')
        self.endorse_df = read_csv(spark, './data/Endorse_use.csv')
        self.primary_person_df = read_csv(spark, './data/Primary_Person_use.csv')
        self.restrict_df = read_csv(spark, './data/Restrict_use.csv')
        self.units_df = read_csv(spark, './data/Units_use.csv')

    def analysis_1_male_deaths(self):
        return self.primary_person_df.filter(
            (col("PRSN_INJRY_SEV_ID") == "KILLED") &
            (col("PRSN_GNDR_ID") == "MALE")
        ).groupBy(col("CRASH_ID")).agg(
            _count(col("CRASH_ID")).alias('numberOfMales')
        ).filter(col("numberOfMales") >= 2)

    def analysis_2_two_wheelers_involved(self):
        return self.units_df.filter(
            (col("UNIT_DESC_ID") == "PEDALCYCLIST") | 
            ((col("UNIT_DESC_ID") == "PEDALCYCLIST") & 
             (col("VEH_BODY_STYL_ID").isin("MOTORCYCLE", "POLICE_MOTORCYCLE")))
        ).count()

    def analysis_3_top_5_vehicle_makes(self):
        driver_air_bag_not_deployed = self.primary_person_df.filter(
            (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") & 
            (col("PRSN_TYPE_ID") == "DRIVER")
        ).dropDuplicates(["CRASH_ID", "UNIT_NBR"]).select("CRASH_ID", "UNIT_NBR")

        filtered_units_df = self.units_df.filter(col("DEATH_CNT") >= 1).select("VEH_MAKE_ID", "DEATH_CNT", "CRASH_ID", "UNIT_NBR")

        return driver_air_bag_not_deployed.join(
            filtered_units_df, 
            ["CRASH_ID", "UNIT_NBR"]
        ).groupBy("VEH_MAKE_ID").sum("DEATH_CNT").sort(
            "sum(DEATH_CNT)", ascending=False
        ).withColumnRenamed("sum(DEATH_CNT)", "TOTAL_DEATHS").limit(5)

    def analysis_4_hit_and_run_valid_license(self):
        hit_and_run_crash_id = self.charges_df.filter(
            col("CHARGE").contains("HIT AND RUN")
        ).dropDuplicates(["crash_id"]).select("crash_id")

        driver_with_valid_license = self.primary_person_df.filter(
            ((col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE") | 
             (col("DRVR_LIC_TYPE_ID") == "COMMERCIAL DRIVER LIC.")) & 
            (col("PRSN_TYPE_ID") == "DRIVER")
        ).select("crash_id")

        return hit_and_run_crash_id.join(
            driver_with_valid_license, "crash_id"
        ).count()

    def analysis_5_female_free_accidents_by_state(self):
        return self.primary_person_df.filter(
            col("PRSN_GNDR_ID") != 'FEMALE'
        ).groupBy("DRVR_LIC_STATE_ID").count().sort(
            "count", ascending=False
        ).limit(1)

    def analysis_6_top_vehicle_makes_injuries(self):
        crash_ids_with_injuries = self.primary_person_df.filter(
            col("PRSN_INJRY_SEV_ID") != "NOT INJURED"
        ).dropDuplicates(["crash_id"]).select("crash_id")

        top_vehicle_makes = crash_ids_with_injuries.join(
            self.units_df, 
            crash_ids_with_injuries.crash_id == self.units_df.CRASH_ID, "inner"
        ).groupBy("VEH_MAKE_ID").count().sort("count", ascending=False)

        window_spec = Window.orderBy(desc("count"))
        return top_vehicle_makes.withColumn(
            "row_num", row_number().over(window_spec)
        ).filter((col("row_num") >= 3) & (col("row_num") <= 5))

    def analysis_7_top_ethnic_group_by_body_style(self):
        units_joined_primary_person = self.units_df.join(
            self.primary_person_df, "CRASH_ID"
        ).groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").agg(
            _count("PRSN_ETHNICITY_ID").alias("count")
        )

        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        return units_joined_primary_person.withColumn(
            "rank", row_number().over(window_spec)
        ).filter(col("rank") == 1).drop("rank", "count")

    def analysis_8_top_5_zip_codes_alcohol_related_crashes(self):
        crash_ids_under_alcohol = self.units_df.filter(
            (col("CONTRIB_FACTR_1_ID") == "UNDER INFLUENCE - ALCOHOL") |
            (col("CONTRIB_FACTR_2_ID") == "UNDER INFLUENCE - ALCOHOL") |
            (col("CONTRIB_FACTR_P1_ID") == "UNDER INFLUENCE - ALCOHOL")
        ).dropDuplicates(["crash_id"]).select("crash_id")

        return crash_ids_under_alcohol.join(
            self.primary_person_df, "CRASH_ID"
        ).groupBy("DRVR_ZIP").count().sort("count", ascending=False).limit(5)

    def analysis_9_no_damage_above_threshold(self):
        return self.units_df.filter(
            (col("VEH_DMAG_AREA_1_ID").isin(
                'NA', 'NOT APPLICABLE (MOTORCYCLE, FARM TRACTOR, ETC.)', 
                'NOT APPLICABLE ( FARM TRACTOR, ETC.)'
            )) &
            (expr("CAST(SUBSTRING(VEH_DMAG_SCL_2_ID, LENGTH(VEH_DMAG_SCL_2_ID), 1) AS INT)") > 4) &
            col("FIN_RESP_TYPE_ID").contains("INSURANCE")
        ).dropDuplicates(["CRASH_ID"]).count()

    def analysis_10_top_5_vehicle_makes_speeding_offences(self):
        top_25_states = self.primary_person_df.groupBy(
            col("DRVR_LIC_STATE_ID")
        ).count().sort("count", ascending=False).limit(25).select("DRVR_LIC_STATE_ID")

        top_10_used_colors = self.units_df.groupBy(
            col("VEH_COLOR_ID")
        ).count().sort("count", ascending=False).limit(10).select("VEH_COLOR_ID")

        speeding_related_charges = self.charges_df.filter(
            col("CHARGE").contains("SPEED")
        ).select("crash_id").dropDuplicates(["crash_id"])

        speeding_units = speeding_related_charges.join(
            self.units_df, speeding_related_charges.crash_id == self.units_df.CRASH_ID
        ).filter(
            (col("VEH_LIC_STATE_ID").isin(top_25_states)) &
            (col("VEH_COLOR_ID").isin(top_10_used_colors)) &
            (col("DRVR_LIC_TYPE_ID").contains("DRIVER LICENSE")) &
            (col("VEH_BODY_STYL_ID") != "NA")
        ).groupBy("VEH_MAKE_ID").count().sort("count", ascending=False).limit(5)

        return speeding_units
