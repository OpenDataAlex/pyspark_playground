# Purpose:  Experimenting with creating a loader for Lahman Baseball dataset.
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from data_vault_loader import DataVaultLoader

spark = SparkSession.builder.master("local").appName("Lahman Teams Load").config("spark.executor.memory", "6gb").getOrCreate()

data_set = spark.read.format("csv").option("header", "true").load("/home/ameadows/Downloads/baseballdatabank-master_2018-03-28/baseballdatabank-master/core/Teams.csv")

oldColumns = data_set.schema.names
newColumns = ["year_id"
              , "league_id"
              , "team_id"
              , 'franchise_id'
              , 'division_id'
              , 'rank'
              , 'games_played'
              , 'home_games_played'
              , 'wins'
              , 'losses'
              , 'division_winner'
              , 'wild_card_winner'
              , 'league_champion'
              , 'world_series_winner'
              , 'runs_scored'
              , 'at_bats'
              , 'hits_by_batters'
              , 'doubles'
              , 'triples'
              , 'homeruns_by_batters'
              , 'walks_by_batters'
              , 'strikeouts_by_batters'
              , 'stolen_bases'
              , 'caught_stealing'
              , 'batters_hit_by_pitch'
              , 'sacrifice_flies'
              , 'opponents_run_scored'
              , 'earned_runs_allowed'
              , 'earned_run_average'
              , 'complete_games'
              , 'shutouts'
              , 'saves'
              , 'outs_pitched'
              , 'hits_allowed'
              , 'homeruns_allowed'
              , 'walks_allowed'
              , 'strikeouts_by_pitchers'
              , 'errors'
              , 'double_plays'
              , 'fielding_percentage'
              , 'team_full_name'
              , 'home_ballpark_name'
              , 'home_attendance_total'
              , 'three_year_park_factor_for_batters'
              , 'three_year_park_factor_for_pitchers'
              , 'baseball_reference_team_id'
              , 'lahman_previous_team_id'
              , 'retrosheet_team_id']

# Change the column names to be cleaner
data_set = reduce(lambda data_set, idx: data_set.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), data_set)


# Change data types to reflect correct types.
data_set = data_set.withColumn('rank', data_set['rank'].cast(IntegerType()))
data_set = data_set.withColumn('games_played', data_set['games_played'].cast(IntegerType()))
data_set = data_set.withColumn('home_games_played', data_set['home_games_played'].cast(IntegerType()))
data_set = data_set.withColumn('wins', data_set['wins'].cast(IntegerType()))
data_set = data_set.withColumn('losses', data_set['losses'].cast(IntegerType()))
data_set = data_set.withColumn('division_winner', data_set['division_winner'].cast(BooleanType()))
data_set = data_set.withColumn('wild_card_winner', data_set['wild_card_winner'].cast(BooleanType()))
data_set = data_set.withColumn('league_champion', data_set['league_champion'].cast(BooleanType()))
data_set = data_set.withColumn('world_series_winner', data_set['world_series_winner'].cast(BooleanType()))
data_set = data_set.withColumn('runs_scored', data_set['runs_scored'].cast(IntegerType()))
data_set = data_set.withColumn('at_bats', data_set['at_bats'].cast(IntegerType()))
data_set = data_set.withColumn('hits_by_batters', data_set['hits_by_batters'].cast(IntegerType()))
data_set = data_set.withColumn('doubles', data_set['doubles'].cast(IntegerType()))
data_set = data_set.withColumn('triples', data_set['triples'].cast(IntegerType()))
data_set = data_set.withColumn('homeruns_by_batters', data_set['homeruns_by_batters'].cast(IntegerType()))
data_set = data_set.withColumn('walks_by_batters', data_set['walks_by_batters'].cast(IntegerType()))
data_set = data_set.withColumn('strikeouts_by_batters', data_set['strikeouts_by_batters'].cast(IntegerType()))
data_set = data_set.withColumn('stolen_bases', data_set['stolen_bases'].cast(IntegerType()))
data_set = data_set.withColumn('caught_stealing', data_set['caught_stealing'].cast(IntegerType()))
data_set = data_set.withColumn('batters_hit_by_pitch', data_set['batters_hit_by_pitch'].cast(IntegerType()))
data_set = data_set.withColumn('sacrifice_flies', data_set['sacrifice_flies'].cast(IntegerType()))
data_set = data_set.withColumn('opponents_run_scored', data_set['opponents_runs_scored'].cast(IntegerType()))
data_set = data_set.withColumn('earned_runs_allowed', data_set['earned_runs_allowed'].cast(IntegerType()))
data_set = data_set.withColumn('complete_games', data_set['complete_games'].cast(IntegerType()))
data_set = data_set.withColumn('shutouts', data_set['shutouts'].cast(IntegerType()))
data_set = data_set.withColumn('saves', data_set['saves'].cast(IntegerType()))
data_set = data_set.withColumn('outs_pitched', data_set['outs_pitched'].cast(IntegerType()))
data_set = data_set.withColumn('hits_allowed', data_set['hits_allowed'].cast(IntegerType()))
data_set = data_set.withColumn('homeruns_allowed', data_set['homeruns_allowed'].cast(IntegerType()))
data_set = data_set.withColumn('walks_allowed', data_set['walks_allowed'].cast(IntegerType()))
data_set = data_set.withColumn('strikeouts_by_pitchers', data_set['strikeouts_by_pitchers'].cast(IntegerType()))
data_set = data_set.withColumn('errors', data_set['errors'].cast(IntegerType()))
data_set = data_set.withColumn('double_plays', data_set['double_plays'].cast(IntegerType()))
data_set = data_set.withColumn('fielding_percentage', data_set['fielding_percentage'].cast(FloatType()))
data_set = data_set.withColumn('home_attendance_total', data_set['home_attendance_total'].cast(IntegerType()))
data_set = data_set.withColumn('runs_scored', data_set['runs_scored'].cast(IntegerType()))
data_set = data_set.withColumn('three_year_park_factor_for_batters', data_set['three_year_park_factor_for_batters'].cast(IntegerType()))
data_set = data_set.withColumn('three_year_park_factor_for_pitchers', data_set['three_year_park_factor_for_pitchers'].cast(IntegerType()))

# Adding Hub UUIDs to the dataset.
data_set = DataVaultLoader().universal_identifier_generator(data_set=data_set, key_field='year_id', key_name='year_uuid')
data_set = DataVaultLoader().universal_identifier_generator(data_set=data_set, key_field='league_id', key_name='league_uuid')
data_set = DataVaultLoader().universal_identifier_generator(data_set=data_set, key_field='team_id', key_name='team_uuid')
data_set = DataVaultLoader().universal_identifier_generator(data_set=data_set, key_field='franchise_id', key_name='franchise_uuid')
data_set = DataVaultLoader().universal_identifier_generator(data_set=data_set, key_field='division_id', key_name='division_uuid')

# Build the Teams Link UUID

data_set = DataVaultLoader().universal_identifier_generator(data_set=data_set, key_field=['year_id', 'league_id', 'team_id', 'franchise_id', 'division_id'], key_name='team_link_uuid')

# Build the Teams Sat Hash
column_list = [ 'rank'
              , 'games_played'
              , 'home_games_played'
              , 'wins'
              , 'losses'
              , 'division_winner'
              , 'wild_card_winner'
              , 'league_champion'
              , 'world_series_winner'
              , 'runs_scored'
              , 'at_bats'
              , 'hits_by_batters'
              , 'doubles'
              , 'triples'
              , 'homeruns_by_batters'
              , 'walks_by_batters'
              , 'strikeouts_by_batters'
              , 'stolen_bases'
              , 'caught_stealing'
              , 'batters_hit_by_pitch'
              , 'sacrifice_flies'
              , 'opponents_runs_scored'
              , 'earned_runs_allowed'
              , 'earned_run_average'
              , 'complete_games'
              , 'shutouts'
              , 'saves'
              , 'outs_pitched'
              , 'hits_allowed'
              , 'homeruns_allowed'
              , 'walks_allowed'
              , 'strikeouts_by_pitchers'
              , 'errors'
              , 'double_plays'
              , 'fielding_percentage'
              , 'team_full_name'
              , 'home_ballpark_name'
              , 'home_attendance_total'
              , 'three_year_park_factor_for_batters'
              , 'three_year_park_factor_for_pitchers'
              , 'baseball_reference_team_id'
              , 'lahman_previous_team_id'
              , 'retrosheet_team_id']

data_set = DataVaultLoader().universal_identifier_generator(data_set=data_set, key_field=column_list, key_name='team_sat_hash')

# Add create audit fields to data_set

data_set = DataVaultLoader().audit_field_manager(data_set=data_set, audit_type='create', process='Spark Test', actor='Me', source='Lahman CSV')
data_set.printSchema()


# Build the Year Hub
year_hub = data_set.select('year_uuid', 'year_id', 'create_actor_id', 'create_date_time', 'create_process_id', 'create_source_id')
year_hub = year_hub.dropDuplicates()
year_hub.write.parquet('/home/ameadows/Documents/testing/data_vault/hub/year_hub.parquet')



# Build the League Hub
league_hub = data_set.select('league_uuid', 'league_id', 'create_actor_id', 'create_date_time', 'create_process_id', 'create_source_id')
league_hub = league_hub.dropDuplicates()
league_hub.write.parquet('/home/ameadows/Documents/testing/data_vault/hub/league_hub.parquet')


# Build the Team Hub

team_hub = data_set.select('team_uuid', 'team_id', 'create_actor_id', 'create_date_time', 'create_process_id', 'create_source_id')
team_hub = team_hub.dropDuplicates()
team_hub.write.parquet('/home/ameadows/Documents/testing/data_vault/hub/team_hub.parquet')


# Build the Franchise Hub

franchise_hub = data_set.select('franchise_uuid', 'franchise_id', 'create_actor_id', 'create_date_time', 'create_process_id', 'create_source_id')
franchise_hub = franchise_hub.dropDuplicates()
franchise_hub.write.parquet('/home/ameadows/Documents/testing/data_vault/hub/franchise_hub.parquet')


# Build the Division Hub

division_hub = data_set.select('division_hub', 'division_id', 'create_actor_id', 'create_date_time', 'create_process_id', 'create_source_id')
division_hub = division_hub.dropDuplicates()
division_hub.write.parquet('/home/ameadows/Documents/testing/data_vault/hub/division_hub.parquet')


# Build the Team Link
team_link = data_set.select('team_link_uuid', 'year_uuid', 'league_uuid', 'team_uuid', 'franchise_uuid', 'division_uuid', 'create_actor_id', 'create_date_time', 'create_process_id', 'create_source_id')
team_link = team_link.dropDuplicates()
team_link.write.parquet('/home/ameadows/Documents/testing/data_vault/link/team_link.parquet')

# Build the Team Sat
team_sat = data_set.select('team_link_uuid'
                           ,'team_sat_hash'
                           , 'rank'
              , 'games_played'
              , 'home_games_played'
              , 'wins'
              , 'losses'
              , 'division_winner'
              , 'wild_card_winner'
              , 'league_champion'
              , 'world_series_winner'
              , 'runs_scored'
              , 'at_bats'
              , 'hits_by_batters'
              , 'doubles'
              , 'triples'
              , 'homeruns_by_batters'
              , 'walks_by_batters'
              , 'strikeouts_by_batters'
              , 'stolen_bases'
              , 'caught_stealing'
              , 'batters_hit_by_pitch'
              , 'sacrifice_flies'
              , 'opponents_runs_scored'
              , 'earned_runs_allowed'
              , 'earned_run_average'
              , 'complete_games'
              , 'shutouts'
              , 'saves'
              , 'outs_pitched'
              , 'hits_allowed'
              , 'homeruns_allowed'
              , 'walks_allowed'
              , 'strikeouts_by_pitchers'
              , 'errors'
              , 'double_plays'
              , 'fielding_percentage'
              , 'team_full_name'
              , 'home_ballpark_name'
              , 'home_attendance_total'
              , 'three_year_park_factor_for_batters'
              , 'three_year_park_factor_for_pitchers'
              , 'baseball_reference_team_id'
              , 'lahman_previous_team_id'
              , 'retrosheet_team_id')
team_sat.write.parquet('/home/ameadows/Documents/testing/data_vault/sat/team_sat.parquet')