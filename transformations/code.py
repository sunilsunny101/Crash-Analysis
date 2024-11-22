import os
from pyspark.sql import SparkSession
from transformations.Functions_pyspark import *


spark = SparkSession.builder.appName("CrashAnalysis_").getOrCreate()
#spark.sparkContext.setLogLevel("ERROR")

current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, "../data")
#output_dir = os.path.join(current_dir, "output")

primary_person_df = createdf(data_dir,'Primary_Person_use.csv')
units_df = createdf(data_dir,'Units_use.csv')
damages_df = createdf(data_dir,'Damages_use.csv')


print('Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?')
df_1 = no_of_males_killed(primary_person_df)
df_1.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')


print('Analysis 2: How many two wheelers are booked for crashes? ')
df_2 = Two_wheeler_crashes(units_df)
df_2.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')


print('Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy. ')
df_3 = Top_5_Veh_Makers(units_df,primary_person_df)
df_3.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')

print('Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? ')
df_4 = Licence_valid_HNR(units_df,primary_person_df)
df_4.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')

print('Analysis 5: Which state has highest number of accidents in which females are not involved?')
df_5 = State_highest_nonFemale_crash(primary_person_df)
df_5.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')

print('Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death ')
df_6 = top_3_to_5_VEH_Makers(units_df)
df_6.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')

print('Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style')
df_7 = top_body_ethnic_grp(units_df,primary_person_df)
df_7.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')

print('Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code) ')
df_8 = Top_5_zip_alcoholic(primary_person_df)
df_8.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')


print('Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance ')
df_9 = No_damageprop_high_vehdamage(units_df,damages_df)
df_9.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')


print('''Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
      used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
       (to be deduced from the data) ''')
df_10 = top_5_vehmak_lic(units_df,primary_person_df)
df_10.show()
print('__________________________________________________________________________________________________________________________________________________________________________________________')

def main():
    print("Hello from main.py!")

if __name__ == "__main__":
    main()


spark.stop()




