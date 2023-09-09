"""
    run_query(query; connection = create_default_connection())

Run a query on the database.
"""
function _run_query(query, connection)
 
    statement = DBInterface.prepare(connection, query)
    results = DBInterface.execute(statement) |> DataFrame
    DBInterface.close!(statement)

    return results
end

"""
    create_default_connection(;database = "data/GDWLND.duckdb")

Create a DBInterface connection to a databse.

# kwargs
- `database`: The path to the database file.
"""
function _create_default_connection(;database = joinpath("data", "GDWLND.duckdb"))
    isdir(dirname(database)) || mkdir(dirname(database))
    return DBInterface.connect(DuckDB.DB, database)
end

######################################
#   Table & Key Sequence Creation    #
######################################

function _create_database_structure(connection)
    # Create the database structure
    _create_tables(connection)
    _create_sequences(connection)
    _import_population_data(connection)
end

function _drop_database_structure()
    connection = _create_default_connection()

    # Drop the database structure
    _drop_tables(connection)
    _drop_sequences(connection)
    _drop_parquet_files()

    DBInterface.close(connection)
end

function _create_tables(connection)
    query_list = []
    append!(query_list, ["CREATE OR REPLACE TABLE PopulationDim (PopulationID USMALLINT PRIMARY KEY, Description VARCHAR)"])
    append!(query_list, ["CREATE OR REPLACE TABLE PopulationLoad (PopulationID USMALLINT, AgentID INT, HouseID INT, AgeRangeID VARCHAR, Sex VARCHAR, IncomeRange VARCHAR, PRIMARY KEY (PopulationID, AgentID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE TownDim (TownID USMALLINT PRIMARY KEY, PopulationID USMALLINT, BusinessCount INT, HouseCount INT, SchoolCount INT, DaycareCount INT, GatheringCount INT, AdultCount INT, ElderCount INT, ChildCount INT, EmptyBusinessCount INT, Model String)"])
    append!(query_list, ["CREATE OR REPLACE TABLE BusinessTypeDim (BusinessTypeID USMALLINT PRIMARY KEY, Description VARCHAR)"])
    append!(query_list, ["CREATE OR REPLACE TABLE BusinessLoad (TownID USMALLINT, BusinessID INT, BusinessTypeID INT, EmployeeCount INT, PRIMARY KEY (TownID, BusinessID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE HouseholdLoad (TownID USMALLINT, HouseholdID INT, ChildCount INT, AdultCount INT, ElderCount INT, PRIMARY KEY (TownID, HouseholdID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE NetworkDim (NetworkID USMALLINT PRIMARY KEY, TownID INT, ConstructionLengthDays INT, Model String)"])
    append!(query_list, ["CREATE OR REPLACE TABLE NetworkSCMLoad (NetworkID USMALLINT, Agent1 INT, Agent2 INT, Weight INT, PRIMARY KEY (NetworkID, Agent1, Agent2))"])
    append!(query_list, ["CREATE OR REPLACE TABLE BehaviorDim (BehaviorID USMALLINT PRIMARY KEY, NetworkID INT, MaskDistributionType VARCHAR , VaxDistributionType VARCHAR, MaskPortion INT, VaxPortion INT)"])
    append!(query_list, ["CREATE OR REPLACE TABLE AgentLoad (BehaviorID UINTEGER, AgentID INT, AgentHouseholdID INT, IsMasking INT, IsVaxed INT, PRIMARY KEY (BehaviorID, AgentID))"])
    append!(query_list, ["CREATE OR REPLACE TABLE EpidemicDim (EpidemicID UINTEGER PRIMARY KEY, BehaviorID UINTEGER, InfectedTotal USMALLINT, InfectedMax USMALLINT, PeakDay USMALLINT, RecoveredTotal USMALLINT, RecoveredMasked USMALLINT, RecoveredVaccinated USMALLINT, RecoveredMaskAndVax USMALLINT)"])
    append!(query_list, ["CREATE OR REPLACE TABLE EpidemicSCMLoad (EpidemicID UINTEGER, Agent1 INT, Agent2 INT, Weight INT, PRIMARY KEY (EpidemicID, Agent1, Agent2))"])
    append!(query_list, ["CREATE OR REPLACE TABLE TransmissionLoad (EpidemicID UINTEGER, AgentID USMALLINT, InfectedBy USMALLINT, InfectionTimeHour UINTEGER)"])
    append!(query_list, ["CREATE OR REPLACE TABLE EpidemicLoad (EpidemicID UINTEGER, Hour USMALLINT, Symptomatic USMALLINT, Recovered USMALLINT, PopulationLiving USMALLINT, PRIMARY KEY (EpidemicID, Hour))"])

    for query in query_list
        _run_query(query, connection)
    end
end

function _vacuum_database()
    connection = _create_default_connection(database=joinpath("data", "vacuumed.db"))

    _run_query("ATTACH '$(joinpath("data","GDWLND.duckdb"))' as source (READ_ONLY);", connection)
    
    _run_query("CREATE TABLE PopulationDim AS SELECT * from source.main.PopulationDim;", connection)
    _run_query("CREATE TABLE PopulationLoad AS SELECT * from source.main.PopulationLoad;", connection)
    _run_query("CREATE TABLE TownDim AS SELECT * from source.main.TownDim;", connection)
    _run_query("CREATE TABLE BusinessLoad AS SELECT * from source.main.BusinessLoad;", connection)
    _run_query("CREATE TABLE HouseholdLoad AS SELECT * from source.main.HouseholdLoad;", connection)
    _run_query("CREATE TABLE NetworkDim AS SELECT * from source.main.NetworkDim;", connection)
    _run_query("CREATE TABLE NetworkSCMLoad AS SELECT * from source.main.NetworkSCMLoad;", connection)
    _run_query("CREATE TABLE BehaviorDim AS SELECT * from source.main.BehaviorDim;", connection)
    _run_query("CREATE TABLE AgentLoad AS SELECT * from source.main.AgentLoad;", connection)
    _run_query("CREATE TABLE EpidemicDim AS SELECT * from source.main.EpidemicDim;", connection)
    _run_query("CREATE TABLE EpidemicLoad AS SELECT * from source.main.EpidemicLoad;", connection)
    _run_query("CREATE TABLE EpidemicSCMLoad AS SELECT * from source.main.EpidemicSCMLoad;", connection)
    _run_query("CREATE TABLE TransmissionLoad AS SELECT * from source.main.TransmissionLoad;", connection)

    val = _run_query("SELECT nextval('source.main.PopulationDimSequence')", connection)[1,1]
    _run_query("CREATE SEQUENCE PopulationDimSequence START $val", connection)
    val = _run_query("SELECT nextval('source.main.TownDimSequence')", connection)[1,1]
    _run_query("CREATE SEQUENCE TownDimSequence START $val", connection)
    val = _run_query("SELECT nextval('source.main.BusinessTypeDimSequence')", connection)[1,1]
    _run_query("CREATE SEQUENCE BusinessTypeDimSequence START $val", connection)
    val = _run_query("SELECT nextval('source.main.NetworkDimSequence')", connection)[1,1]
    _run_query("CREATE SEQUENCE NetworkDimSequence START $val", connection)
    val = _run_query("SELECT nextval('source.main.BehaviorDimSequence')", connection)[1,1]
    _run_query("CREATE SEQUENCE BehaviorDimSequence START $val", connection)
    val = _run_query("SELECT nextval('source.main.EpidemicDimSequence')", connection)[1,1]
    _run_query("CREATE SEQUENCE EpidemicDimSequence START $val", connection)

    _run_query("DETACH source;", connection)
    
    DBInterface.close(connection)

    mv(joinpath("data", "vacuumed.db"), joinpath("data", "GDWLND.duckdb"), force=true)
end

function _get_model_by_town_id(townId::Int, connection)
    try
        model = _run_query("SELECT Model FROM TownDim WHERE TownID = $townId", connection)[1,1]
        model = parse.(UInt8, split(model, ","))
        return Deserialize_Model(convert.(UInt8, model))
    catch e
        @warn "Failed to extract model with TownId $townId"
        return nothing
    end
end

function _get_model_by_network_id(networkId::Int, connection)
    try
        model = _run_query("SELECT Model FROM NetworkDim WHERE NetworkID = $networkId", connection)[1,1]
        model = parse.(UInt8, split(model, ","))
        return Deserialize_Model(convert.(UInt8, model))
    catch e
        @warn "Failed to extract model with NetworkId $networkId"
        return nothing
    end
end

function _get_model_by_behavior_id(behaviorId::Int, connection)
    query = """
        SELECT NetworkID, MaskDistributionType, VaxDistributionType, MaskPortion, VaxPortion 
        FROM BehaviorDim
        WHERE BehaviorID = $behaviorId
    """
    behaviorData = run_query(query, connection)
    networkId = Int(behaviorData[1,1])
    maskDistributionType = behaviorData[1,2]
    vaxDistributionType = behaviorData[1,3]
    maskPortion = Int(behaviorData[1,4])
    vaxPortion = Int(behaviorData[1,5])

    model = _get_model_by_network_id(networkId, connection)
    model === nothing && return nothing

    model.behavior_id = behaviorId
    model.mask_distribution_type = maskDistributionType
    model.vax_distribution_type = vaxDistributionType
    model.mask_portion = maskPortion
    model.vax_portion = vaxPortion

    # Apply social behavior
    query = """
        SELECT AgentID
        FROM AgentLoad
        WHERE BehaviorID = $(model.behavior_id)
        AND IsMasking = 1
    """
    maskIdArr = run_query(query, connection)[!,1]

    query = """
        SELECT AgentID
        FROM AgentLoad
        WHERE BehaviorID = $(model.behavior_id)
        AND IsVaxed = 1
    """
    vaxIdArr = run_query(query, connection)[!,1]

    Update_Agents_Attribute!(model, maskIdArr, :will_mask, [true, true, true])
    Update_Agents_Attribute!(model, vaxIdArr, :status, :V)
    Update_Agents_Attribute!(model, vaxIdArr, :vaccinated, true)

    return model
end

function _drop_tables(connection)
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
    append!(query_list, ["DROP TABLE IF EXISTS AgentLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS EpidemicDim"])
    append!(query_list, ["DROP TABLE IF EXISTS TransmissionLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS EpidemicLoad"])
    append!(query_list, ["DROP TABLE IF EXISTS EpidemicSCMLoad"])

    for query in query_list
        _run_query(query, connection)
    end
end

function _create_sequences(connection)
    query_list = []
    append!(query_list, ["CREATE SEQUENCE PopulationDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE TownDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE BusinessTypeDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE NetworkDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE BehaviorDimSequence START 1"])
    append!(query_list, ["CREATE SEQUENCE EpidemicDimSequence START 1"])

    for query in query_list
        _run_query(query, connection)
    end
end

function _drop_sequences(connection)
    query_list = []
    append!(query_list, ["DROP SEQUENCE IF EXISTS PopulationDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS TownDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS BusinessTypeDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS NetworkDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS BehaviorDimSequence"])
    append!(query_list, ["DROP SEQUENCE IF EXISTS EpidemicDimSequence"])

    for query in query_list
        _run_query(query, connection)
    end
end

function _verify_database_structure()
    # Verify that the database has the correct structure TODO
    return true
end

function _import_population_data(connection)
    _import_small_town_population(connection)
    _import_large_town_population(connection)
end

function _import_small_town_population(connection)
    if !isdir(joinpath("lib","RuralABM","data","example_towns","small_town")) 
        @warn "The small town data directory does not exist"
        return
    end

    # Load Data
    population = DataFrame(CSV.File(joinpath("lib","RuralABM","data","example_towns","small_town","population.csv")))

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
    Result = _run_query(query, connection)
    PopulationID = Result[1, 1]

    # Add Population data to PopulationDim
    appender = DuckDB.Appender(connection, "PopulationLoad")

    for row in eachrow(population)
        row[2] == "NA" && continue
        DuckDB.append(appender, PopulationID)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.append(appender, row[5])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)
end

function _import_large_town_population(connection)
    if !isdir(joinpath("lib","RuralABM","data","example_towns","large_town")) 
        @warn "The large town data directory does not exist"
        return
    end

    # Load Data
    population = DataFrame(CSV.File(joinpath("lib","RuralABM","data","example_towns","large_town","population.csv")))

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
    Result = _run_query(query, connection) |> DataFrame
    PopulationID = Result[1, 1]

    # Add Population data to PopulationDim
    appender = DuckDB.Appender(connection, "PopulationLoad")

    for row in eachrow(population)
        row[2] == "NA" && continue
        DuckDB.append(appender, PopulationID)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.append(appender, row[5])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)
end

function _drop_parquet_files()
    try
        rm(joinpath("data","EpidemicSCMLoad"), recursive = true)
    catch
        @warn "EpidemicSCMLoad parquet file does not exist"
    end
end

function _export_database(filepath, connection)
    _run_query("EXPORT DATABASE '$(filepath)' (FORMAT PARQUET)", connection)
end
