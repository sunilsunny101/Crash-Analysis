from pyspark.sql.functions import   col, count, sum, dense_rank
from pyspark.sql.window import Window
from pyspark.sql import SparkSession



spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()


def createdf(filepath, filename):
    """
    Creates a DataFrame by reading a CSV file from the specified path.
    
    Args:
        filepath (str): Path to the folder containing the data file.
        filename (str): Name of the CSV file to be read.
    
    Returns:
        DataFrame: The loaded DataFrame from the CSV file.
    """
    try:
        return spark.read.format("csv").option('Header', True).option('inferSchema', True).option('sep', ',').load(filepath+"/" + filename, escape="\n", multiLine=True)
    except Exception as e:
        print(f"Error reading file: {e}")
        return None


def no_of_males_killed(df):
    """
    Calculates the number of crashes where more than 2 males were killed.
    
    Args:
        df (DataFrame): The DataFrame containing crash data. It takes Primary Person use data.
    
    Returns:
        DataFrame: A DataFrame containing the number of crashes with more than 2 males killed.
    """
    if df is not None:
        num_males_killed_df = df.filter(df['PRSN_GNDR_ID'] == 'MALE') \
                               .groupBy('CRASH_ID') \
                               .agg(sum('DEATH_CNT').alias('MALE_DEATH_CNT')) \
                               .filter('MALE_DEATH_CNT > 2') \
                               .agg(count('CRASH_ID').alias('num_male_killed'))
        return num_males_killed_df.show()
    return None

def Two_wheeler_crashes(df):
    """
    Counts the number of crashes involving two-wheeled vehicles (motorcycles).
    
    Args:
        df (DataFrame): The DataFrame containing crash data. It takes data from Units_use.csv file.
    
    Returns:
        DataFrame: A DataFrame with the count of motorcycle crashes.
    """
    if df is not None:
        two_wheelers_df = df.filter(col('VEH_BODY_STYL_ID').like('%MOTORCYCLE%')) \
                            .agg(count('CRASH_ID').alias('Motorcycle_crash_count'))
        return two_wheelers_df.show()
    return None

def Top_5_Veh_Makers(unitdf, persondf):
    """
    Identifies the top 5 vehicle makes involved in crashes where the driver died and airbags were not deployed.
    
    Args:
        unitdf (DataFrame): The DataFrame containing vehicle data.(Units_use.csv)
        persondf (DataFrame): The DataFrame containing person data.(Primary_Person_use.csv)
    
    Returns:
        DataFrame: A DataFrame containing the top 5 vehicle makes involved in such crashes.
    """
    if unitdf is not None and persondf is not None:
        unit_primary_joined_df = unitdf.alias('e1').join(persondf.alias('e2'), on='CRASH_ID', how='inner')
        filtered_df = unit_primary_joined_df.filter((col('PRSN_TYPE_ID') == 'DRIVER') & (col('e1.DEATH_CNT') > 0) & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')) \
                                              .filter(~col('VEH_BODY_STYL_ID').like('%MOTORCYCLE%'))
        top_5_vehicle_makes_df = filtered_df.groupBy('VEH_MAKE_ID') \
                                            .agg(count('e1.CRASH_ID').alias('crash_count')) \
                                            .orderBy(col('crash_count').desc()) \
                                            .limit(5)
        return top_5_vehicle_makes_df.show()
    return None

def Licence_valid_HNR(unitdf, persondf):
    """
    Counts how many valid driver license holders were involved in crashes with hit-and-run incidents.
    
    Args:
        unitdf (DataFrame): The DataFrame containing vehicle data.(Units_use.csv)
        persondf (DataFrame): The DataFrame containing person data.(Primary_Person_use.csv)
    
    Returns:
        DataFrame: A DataFrame with the count of valid driver license holders involved in hit-and-run incidents.
    """
    if unitdf is not None and persondf is not None:
        unit_primary_joined_df = unitdf.alias('e1').join(persondf.alias('e2'), on='CRASH_ID', how='inner')
        valid_licence_hit_run_df = unit_primary_joined_df.filter((col('e2.DRVR_LIC_TYPE_ID') == 'DRIVER LICENSE') &
                                                                  (col('e1.VEH_HNR_FL') == 'Y')) \
                                                          .agg(count('e1.CRASH_ID').alias('HNR_COUNT'))
        return valid_licence_hit_run_df.show()
    return None

def State_highest_nonFemale_crash(df):
    """
    Identifies the state with the highest number of crashes involving non-female drivers.
    
    Args:
        df (DataFrame): The DataFrame containing crash data.(Primary_Person_use.csv)
    
    Returns:
        DataFrame: A DataFrame containing the state with the highest non-female crashes.
    """
    if df is not None:
        state_highest_accidents_df = df.filter(col('PRSN_GNDR_ID') != 'FEMALE') \
                                        .groupBy('DRVR_LIC_STATE_ID') \
                                        .agg(count(col('CRASH_ID')).alias('No_of_accidents')) \
                                        .orderBy(col('No_of_accidents').desc()) \
                                        .limit(1)
        return state_highest_accidents_df.show()
    return None

def top_3_to_5_VEH_Makers(df):
    """
    Identifies the top 3 to 5 vehicle makers involved in crashes with the most injuries and deaths.
    
    Args:
        df (DataFrame): The DataFrame containing crash data.(Units_use.csv)
    
    Returns:
        DataFrame: A DataFrame containing the top 3 to 5 vehicle makes based on injury/death count.
    """
    if df is not None:
        window_spec = Window.orderBy(col('INJURY_DEATH_CNT').desc())
        large_no_of_injuries = df.withColumn('INJURY_DEATH_CNT', col('TOT_INJRY_CNT') + col('DEATH_CNT')) \
                                  .groupBy('VEH_MAKE_ID') \
                                  .agg(sum('INJURY_DEATH_CNT').alias('INJURY_DEATH_CNT')) \
                                  .orderBy(col('INJURY_DEATH_CNT').desc())
        top_3_to_5_veh = large_no_of_injuries.withColumn('rank', dense_rank().over(window_spec)) \
                                               .filter(col('rank').between(3, 5)) \
                                               .select('VEH_MAKE_ID', 'INJURY_DEATH_CNT')
        return top_3_to_5_veh.show()
    return None

def top_body_ethnic_grp(unitdf, persondf):
    """
    Identifies the top ethnic group and body style of vehicles involved in the most crashes.
    
    Args:
        unitdf (DataFrame): The DataFrame containing vehicle data.(Units_use.csv)
        persondf (DataFrame): The DataFrame containing person data.(Primary_Person_use.csv)
    
    Returns:
        DataFrame: A DataFrame with the top ethnic group and body style with the highest accident count.
    """
    if unitdf is not None and persondf is not None:
        unit_primary_joined_df = unitdf.join(persondf.alias('e2'), on='CRASH_ID', how='inner').alias('e1')
        top_ethnic_user_group_df = unit_primary_joined_df.groupBy(col('VEH_BODY_STYL_ID'), col('PRSN_ETHNICITY_ID')) \
                                                         .agg(count(col('e1.CRASH_ID')).alias('ETHNICITY_COUNT')) \
                                                         .orderBy(col('ETHNICITY_COUNT').desc()) \
                                                         .limit(1)
        return top_ethnic_user_group_df.show()
    return None

def Top_5_zip_alcoholic(df):
    """
    Identifies the top 5 zip codes with the most alcohol-related crashes.
    
    Args:
        df (DataFrame): The DataFrame containing crash data.(Primary_Person_use.csv)
    
    Returns:
        DataFrame: A DataFrame with the top 5 zip codes for alcohol-related crashes.
    """
    if df is not None:
        top_5_zip_codes_alcohol_df = df.filter(col('PRSN_ALC_RSLT_ID') == 'Positive') \
                                        .groupBy('DRVR_ZIP') \
                                        .agg(count(col('CRASH_ID')).alias('ACCIDENT_COUNT')) \
                                        .orderBy(col('ACCIDENT_COUNT').desc()) \
                                        .filter(col('DRVR_ZIP').isNotNull()) \
                                        .limit(5)
        return top_5_zip_codes_alcohol_df.show()
    return None

def No_damageprop_high_vehdamage(unitdf, damagesdf):
    """
    Identifies crashes with no property damage and high vehicle damage.
    
    Args:
        unitdf (DataFrame): The DataFrame containing vehicle data.(Units_use.csv)
        damagesdf (DataFrame): The DataFrame containing damage information.(Damages_use.csv)
    
    Returns:
        DataFrame: A DataFrame with the count of crashes that meet the criteria.
    """
    if unitdf is not None and damagesdf is not None:
        distinct_crash_ids_df = unitdf.alias('e1').join(damagesdf.alias('e3'), on='CRASH_ID', how='inner') \
                                             .filter(col('e3.DAMAGED_PROPERTY').isin(['NONE', 'NONE1'])) \
                                             .filter(col('e1.FIN_RESP_PROOF_ID') == 1) \
                                             .filter(col('e1.VEH_DMAG_SCL_1_ID').isin(['DAMAGED 5', 'DAMAGED 6'])) \
                                             .agg(count('CRASH_ID').alias('NO_DAMAGE_COUNT'))
        return distinct_crash_ids_df.show()
    return None

def top_5_vehmak_lic(unitdf, persondf):
    """
    Identifies the top 5 vehicle makes involved in crashes with speed as a contributing factor and licensed drivers.
    
    Args:
        unitdf (DataFrame): The DataFrame containing vehicle data.(Units_use.csv)
        persondf (DataFrame): The DataFrame containing person data.(Primary_Person_use.csv)
    
    Returns:
        DataFrame: A DataFrame with the top 5 vehicle makes based on the criteria.
    """
    if unitdf is not None and persondf is not None:
        # Get top 25 states with most accidents
        top_25_states_df = unitdf.groupBy(col('VEH_LIC_STATE_ID')) \
                                 .agg(count(col('CRASH_ID')).alias('crash_count')) \
                                 .orderBy(col('crash_count').desc()) \
                                 .limit(25)
        
        # Get top 10 vehicle colors with the most accidents
        top_10_vehicle_colours_df = unitdf.groupBy(col('VEH_COLOR_ID')) \
                                          .agg(count(col('CRASH_ID')).alias('crash_count_colour')) \
                                          .orderBy(col('crash_count_colour').desc()) \
                                          .limit(10)
        
        # Identify top 5 vehicle makes with speed as contributing factor
        top_5_vehicle_makes_speed_df = (unitdf.alias('e1')
                                        .join(persondf.alias('e2'), 'CRASH_ID', 'inner')
                                        .filter(col('e2.DRVR_LIC_TYPE_ID') == 'DRIVER LICENSE')
                                        .filter(col('e1.CONTRIB_FACTR_1_ID').like('%SPEED%'))
                                        .filter((col('e1.VEH_LIC_STATE_ID').isin([row['VEH_LIC_STATE_ID'] for row in top_25_states_df.collect()])) &
                                                col('e1.VEH_COLOR_ID').isin([row['VEH_COLOR_ID'] for row in top_10_vehicle_colours_df.collect()]))
                                        .groupBy('VEH_MAKE_ID')
                                        .agg(count(col('e1.CRASH_ID')).alias('crash_count'))
                                        .orderBy(col('crash_count').desc())
                                        .limit(5))
        
        return top_5_vehicle_makes_speed_df.show()
    return None