from utils import create_spark_session
from analysis import SparkCrashAnalysis

if __name__ == "__main__":
    
    spark = create_spark_session("CrashAnalysis")
    analysis = SparkCrashAnalysis(spark)

    print("Analysis 1: Number of crashes with more than 2 males killed")
    analysis.analysis_1_male_deaths().show(truncate=False)

    print("Analysis 2: Number of two-wheelers involved in crashes")
    print(analysis.analysis_2_two_wheelers_involved())

    print("Analysis 3: Top 5 vehicle makes where driver died and airbags did not deploy")
    analysis.analysis_3_top_5_vehicle_makes().show(truncate=False)

    print("Analysis 4: Number of vehicles involved in hit and run with a valid driver license")
    print(analysis.analysis_4_hit_and_run_valid_license())

    print("Analysis 5: State with highest number of accidents where no females were involved")
    analysis.analysis_5_female_free_accidents_by_state().show(truncate=False)

    print("Analysis 6: Top 3rd to 5th vehicle makes involved in crashes with injuries or fatalities")
    analysis.analysis_6_top_vehicle_makes_injuries().show(truncate=False)

    print("Analysis 7: Top ethnic group by vehicle body style involved in crashes")
    analysis.analysis_7_top_ethnic_group_by_body_style().show(truncate=False)

    print("Analysis 8: Top 5 zip codes with the highest number of alcohol-related crashes")
    analysis.analysis_8_top_5_zip_codes_alcohol_related_crashes().show(truncate=False)

    print("Analysis 9: Count of distinct crash IDs with no damage observed, damage level above 4, and car insured")
    analysis.analysis_9_no_damage_above_threshold().show(truncate=False)

    print("Analysis 10: Top 5 vehicle makes with speeding offences, licensed drivers, top vehicle colors, and top states")
    analysis.analysis_10_top_5_vehicle_makes_speeding_offences().show(truncate=False)
