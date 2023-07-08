"""
    run_query(query; connection = create_default_connection())

Run a query on the database.
"""
function _run_query(query; connection = _create_default_connection())
    DBInterface.execute(connection, query)
end

"""
    create_default_connection(;database = "data/GDWLND.duckdb")

Create a DBInterface connection to a databse.

# kwargs
- `database`: The path to the database file.
"""
function _create_default_connection(;database = "data/GDWLND.duckdb")
    isdir(dirname(database)) || mkdir(dirname(database))
    return DBInterface.connect(DuckDB.DB, database)
end

######################################
#   Table & Key Sequence Creation    #
######################################

function _create_database_structure()
    # Create the database structure
    _create_tables()
    _create_sequences()
    _import_population_data()
end

function _drop_database_structure()
    # Drop the database structure
    _drop_tables()
    _drop_sequences()
    _drop_parquet_files()
end

function _create_tables()
    query_list = []
    append!(query_list, ["CREATE OR REPLACE TABLE PopulationDim (PopulationID USMALLINT PRIMARY KEY, Description VARCHAR)"])
    append!(query_list, ["CREATE OR REPLACE TABLE PopulationLoad (PopulationID USMALLINT, AgentID INT, HouseID INT, AgeRangeID VARCHAR, Sex VARCHAR, IncomeRange VARCHAR, PRIMARY KEY (PopulationID, AgentID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE TownDim (TownID USMALLINT PRIMARY KEY, PopulationID USMALLINT, BusinessCount INT, HouseCount INT, SchoolCount INT, DaycareCount INT, GatheringCount INT, AdultCount INT, ElderCount INT, ChildCount INT, EmptyBusinessCount INT, MaskDistributionType VARCHAR , VaxDistributionType VARCHAR)"])
    append!(query_list, ["CREATE OR REPLACE TABLE BusinessTypeDim (BusinessTypeID USMALLINT PRIMARY KEY, Description VARCHAR)"])
    append!(query_list, ["CREATE OR REPLACE TABLE BusinessLoad (TownID USMALLINT, BusinessID INT, BusinessTypeID INT, EmployeeCount INT, PRIMARY KEY (TownID, BusinessID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE HouseholdLoad (TownID USMALLINT, HouseholdID INT, ChildCount INT, AdultCount INT, ElderCount INT, PRIMARY KEY (TownID, HouseholdID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE NetworkDim (NetworkID USMALLINT PRIMARY KEY, TownID INT, ConstructionLengthDays INT)"])
    append!(query_list, ["CREATE OR REPLACE TABLE NetworkSCMLoad (NetworkID USMALLINT, Agent1 INT, Agent2 INT, Weight INT, PRIMARY KEY (NetworkID, Agent1, Agent2))"])
    append!(query_list, ["CREATE OR REPLACE TABLE BehaviorDim (BehaviorID USMALLINT PRIMARY KEY, NetworkID INT, MaskVaxID INT)"])
    append!(query_list, ["CREATE OR REPLACE TABLE MaskVaxDim (MaskVaxID USMALLINT PRIMARY KEY, MaskPortion UTINYINT, VaxPortion UTINYINT)"])
    append!(query_list, ["CREATE OR REPLACE TABLE AgentLoad (BehaviorID UINTEGER, AgentID INT, AgentHouseholdID INT, IsMasking INT, IsVaxed INT, PRIMARY KEY (BehaviorID, AgentID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE EpidemicDim (EpidemicID UINTEGER PRIMARY KEY, BehaviorID UINTEGER, InfectedTotal USMALLINT, InfectedMax USMALLINT, PeakDay USMALLINT, RecoveredTotal USMALLINT, RecoveredMasked USMALLINT, RecoveredVaccinated USMALLINT, RecoveredMaskAndVax USMALLINT)"])
    append!(query_list, ["CREATE OR REPLACE TABLE EpidemicSCMLoad (EpidemicID UINTEGER PRIMARY KEY, SCM STRING)"])
    append!(query_list, ["CREATE OR REPLACE TABLE TransmissionLoad (EpidemicID UINTEGER, AgentID USMALLINT, InfectedBy USMALLINT, InfectionTimeHour UINTEGER)"])
    append!(query_list, ["CREATE OR REPLACE TABLE EpidemicLoad (EpidemicID UINTEGER, Day USMALLINT, Symptomatic USMALLINT, Recovered USMALLINT, PopulationLiving USMALLINT, PRIMARY KEY (EpidemicID, Day))"])

    for query in query_list
        _run_query(query)
    end
end

function _drop_tables()
    query_list = []
    append!(query_list, ["DROP TABLE IF EXISTS PopulationDim"])
    append!(query_list, ["DROP TABLE IF EXISTS PopulationLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS TownDim"])
    append!(query_list, ["DROP TABLE IF EXISTS BusinessTypeDim"])
    append!(query_list, ["DROP TABLE IF EXISTS BusinessLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS HouseholdLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS NetworkDim"])
    append!(query_list, ["DROP TABLE IF EXISTS NetworkSCMLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS BehaviorDim"])
    append!(query_list, ["DROP TABLE IF EXISTS MaskVaxDim"])
    append!(query_list, ["DROP TABLE IF EXISTS AgentLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS EpidemicDim"])
    append!(query_list, ["DROP TABLE IF EXISTS TransmissionLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS EpidemicLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS EpidemicSCMLoad"])

    for query in query_list
        _run_query(query)
    end
end

function _create_sequences()
    query_list = []
    append!(query_list, ["CREATE SEQUENCE PopulationDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE TownDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE BusinessTypeDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE NetworkDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE BehaviorDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE MaskVaxDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE EpidemicDimSequence START 1"])

    for query in query_list
        _run_query(query)
    end
end

function _drop_sequences()
    query_list = []
    append!(query_list, ["DROP SEQUENCE IF EXISTS PopulationDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS TownDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS BusinessTypeDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS NetworkDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS BehaviorDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS MaskVaxDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS EpidemicDimSequence"])

    for query in query_list
        _run_query(query)
    end
end

function _verify_database_structure()
    # Verify that the database has the correct structure TODO
    return true
end

function _import_population_data()
    _import_small_town_population()
    _import_large_town_population()
end

function _import_small_town_population()
    if !isdir("lib/RuralABM/data/example_towns/small_town") 
        @warn "The small town data directory does not exist"
        return
    end

    # Load Data
    population = DataFrame(CSV.File("lib/RuralABM/data/example_towns/small_town/population.csv"))

    # Formate Data
    select!(population, Not([:Ind]))
    rename!(population, :Column1 => :AgentID)
    rename!(population, :house => :HouseID)
    rename!(population, :age => :AgeRange)
    rename!(population, :sex => :Sex)
    rename!(population, :income => :IncomeRange)

    # Add Population to PopulationDim
    query = """
        INSERT INTO PopulationDim
        SELECT nextval('PopulationDimSequence') AS PopulationID, 'A small town of 386 residents' AS Description
        RETURNING PopulationID
    """
    Result = _run_query(query) |> DataFrame
    PopulationID = Result[1, 1]

    # Add Population data to PopulationDim
    connection = _create_default_connection()
    DuckDB.register_data_frame(connection, population, "population")
    query = """
        INSERT INTO PopulationLoad 
        SELECT $PopulationID, * 
        FROM population 
        WHERE HouseID <> 'NA'
    """
    _run_query(query, connection = connection)
    _run_query("DROP VIEW population", connection = connection)
    DuckDB.close(connection)
end

function _import_large_town_population()
    if !isdir("lib/RuralABM/data/example_towns/large_town") 
        @warn "The large town data directory does not exist"
        return
    end

    # Load Data
    population = DataFrame(CSV.File("lib/RuralABM/data/example_towns/large_town/population.csv"))

    # Formate Data
    select!(population, Not([:Ind]))
    rename!(population, :Column1 => :AgentID)
    rename!(population, :house => :HouseID)
    rename!(population, :age => :AgeRange)
    rename!(population, :sex => :Sex)
    rename!(population, :income => :IncomeRange)

    # Add Population to PopulationDim
    query = """
        INSERT INTO PopulationDim
        SELECT nextval('PopulationDimSequence') AS PopulationID, 'A large town of 5129 residents' AS Description
        RETURNING PopulationID
    """
    Result = _run_query(query) |> DataFrame
    PopulationID = Result[1, 1]

    # Add Population data to PopulationDim
    connection = _create_default_connection()
    DuckDB.register_data_frame(connection, population, "population")
    query = """
        INSERT INTO PopulationLoad 
        SELECT $PopulationID, * 
        FROM population 
        WHERE HouseID <> 'NA'
    """
    _run_query(query, connection = connection)
    _run_query("DROP VIEW population", connection = connection)
    DuckDB.close(connection)
end

function _drop_parquet_files()
    try
        rm("data/EpidemicSCMLoad", recursive = true)
    catch
        @warn "EpidemicSCMLoad parquet file does not exist"
    end
end

function _export_database(filepath, connection)
    _run_query("EXPORT DATABASE '$(filepath)' (FORMAT PARQUET)", connection = connection)
end

# support for running multiple queries without closing and opening the connection

# Delete directory

