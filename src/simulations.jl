
""" 
    run_ruralABM()

Run the RuralABM package with default parameters.

# Arguments
- `SOCIAL_NETWORKS=10`: Multiplicity of creating town social networks (Range: 1 -> infty).
- `NETWORK_LENGTH=30`: Length in days the model will be run to generate a social network (Range: 1 -> infty).
- `MASKING_LEVELS=5`: Evenly split going from 0 to 100 (exclusive) i.e "2" generates [0,50] (Range: 0 -> 100).
- `VACCINATION_LEVELS=5`: Evenly split going from 0 to 100 (exclusive) i.e "4" generates [0,25,50,75] (Range: 0 -> 100).
- `MODEL_RUNS=100`: Multiplicity model runs with disease spread (Range: 1 -> infty).
- `TOWN_NAMES=["Dubois"]`: Towns which will be run. Ensure input data exist for target towns.
"""
function _run_ruralABM(;
    SOCIAL_NETWORKS::Int = 10,
    NETWORK_LENGTH::Int = 30,
    MASKING_LEVELS::Int = 5,
    VACCINATION_LEVELS::Int = 5,
    DISTRIBUTION_TYPE = [0, 0],
    MODEL_RUNS::Int = 100,
    TOWN_NAMES = "small",
    STORE_NETWORK_SCM = true,
    STORE_EPIDEMIC_SCM = true,
    NUMBER_WORKERS = 5
    )
    # Verify input parameters
    @assert SOCIAL_NETWORKS > 0 "SOCIAL_NETWORKS must be greater than 0"
    @assert NETWORK_LENGTH >= 0 "NETWORK_LENGTH must be greater than or equal to 0"
    NETWORK_LENGTH == 0 && @warn "NETWORK_LENGTH is 0, social networks will not form before disease spread"
    @assert 1 <= MASKING_LEVELS <= 100 "MASKING_LEVELS must be greater than or equal to 0 and less than or equal to 100"
    @assert 1 <= VACCINATION_LEVELS <= 100 "VACCINATION_LEVELS must be greater than or equal to 0 and less than or equal to 100"
    @assert DISTRIBUTION_TYPE[1] in [0, 1] "DISTRIBUTION_TYPE[1] must be 0 or 1"
    @assert DISTRIBUTION_TYPE[2] in [0, 1] "DISTRIBUTION_TYPE[2] must be 0 or 1"
    @assert MODEL_RUNS > 0 "MODEL_RUNS must be greater than 0"

    # Check for database existence
    if !isfile(joinpath("data", "GDWLND.duckdb"))
        println("Creating Database Structure")
        create_database_structure()
    end

    # Run simulations in parallel
    println("Starting Simulation")
    _begin_simulations_faster(SOCIAL_NETWORKS, MASKING_LEVELS, VACCINATION_LEVELS, DISTRIBUTION_TYPE, MODEL_RUNS, NETWORK_LENGTH, TOWN_NAMES, STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM, NUMBER_WORKERS)

    # Vacuum database
    println("Vacuuming Database")
    _vacuum_database()

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

"""
    begin_simulations(town_networks:, mask_levels, vaccine_levels, runs, duration_days_network, towns)

Run RuralABM simulations based on the values passed. See documentation of Run_RuralABM for details.
"""
function _begin_simulations(connection, town_networks::Int, mask_levels::Int, vaccine_levels::Int, distribution_type::Vector{Int64}, runs::Int, duration_days_network, towns; STORE_NETWORK_SCM::Bool = true, STORE_EPIDEMIC_SCM::Bool = true)
    # Compute target levels for masks and vaccines
    mask_incr = floor(100/mask_levels)
    vacc_incr = floor(100/vaccine_levels)

    distribution_type[1] == 0 ? MaskDistributionType = "Random" : MaskDistributionType = "Watts"
    distribution_type[2] == 0 ? VaxDistributionType = "Random" : VaxDistributionType = "Watts"

    for town in towns
        # Get PopulationID
        if town == "small"
            PopulationID = 1
        elseif town == "large"
            PopulationID = 2
        end

        # Build Town Model
        model_raw , townDataSummaryDF, businessStructureDF, houseStructureDF = Construct_Town("lib/RuralABM/data/example_towns/$(town)_town/population.csv", "lib/RuralABM/data/example_towns/$(town)_town/businesses.csv")

        # Insert Town Structure Data
        DuckDB.register_data_frame(connection, townDataSummaryDF, "TownSummaryDF")
        query = """
            INSERT INTO main.TownDim 
            SELECT 
                nextval('TownDimSequence') AS TownID,
                $PopulationID,
                *,
                '$MaskDistributionType',
                '$VaxDistributionType' 
            FROM TownSummaryDF
            RETURNING TownID
        """
        Result = _run_query(query, connection) 
        TownID = Result[1,1]
        _run_query("DROP VIEW TownSummaryDF", connection)


        # Insert business structure data
        DuckDB.register_data_frame(connection, businessStructureDF, "BusinessStructureDF")
        query = """
            INSERT INTO main.BusinessLoad
            SELECT
                $TownID,
                *,
            FROM BusinessStructureDF
        """
        _run_query(query, connection)
        _run_query("DROP VIEW BusinessStructureDF", connection)

        # Insert household structure data 
        DuckDB.register_data_frame(connection, houseStructureDF, "HouseStructureDF")
        query = """
            INSERT INTO main.HouseholdLoad
            SELECT
                $TownID,
                *,
            FROM HouseStructureDF
        """
        _run_query(query, connection)
        _run_query("DROP VIEW HouseStructureDF", connection)

        # Generate social network models and collect compact adjacency matrices
        ResultsPostSocialNetworks = pmap((x,y) -> Run_Model!(x, duration = y), [deepcopy(model_raw) for x in 1:town_networks], fill(duration_days_network, town_networks))
        ModelSocialNetworks = [x[1] for x in ResultsPostSocialNetworks]
        SocialContactMatrices0 = [x[4] for x in ResultsPostSocialNetworks]

        # Label matrices by index and convert to dataframe
        SocialContactMatrices0DF = DataFrame(SocialContactMatrices0, :auto)

        # Store social network contact matrices
        NetworkdIDs = []
        for SocialContactMatrix in eachcol(SocialContactMatrices0DF)
            # Populate NetworkDim
            query = """
                INSERT INTO NetworkDim
                SELECT
                    nextval('NetworkDimSequence') AS NetworkID,
                    $TownID,
                    $duration_days_network
                RETURNING NetworkID
            """
            Results = _run_query(query, connection) 
            NetworkID = Results[1,1]
            append!(NetworkdIDs, NetworkID)

            if STORE_NETWORK_SCM
                # Populate NetworkSCMLoad
                NetworkSCMAppender = DuckDB.Appender(connection, "NetworkSCMLoad")
                
                Population = SocialContactMatrix[1] 
                NetworkSCMItr = 2
                for agent1 in 1:Population
                    for agent2 in agent1+1:Population
                        DuckDB.append(NetworkSCMAppender, NetworkID)
                        DuckDB.append(NetworkSCMAppender, agent1)
                        DuckDB.append(NetworkSCMAppender, agent2)
                        DuckDB.append(NetworkSCMAppender, SocialContactMatrix[NetworkSCMItr])
                        DuckDB.end_row(NetworkSCMAppender)
                        NetworkSCMItr += 1
                    end
                end
                DuckDB.close(NetworkSCMAppender)
            end
        end
        
        SocialNetworkIndex = 1
        for ModelSocialNetwork in ModelSocialNetworks
            # Initialize dataframe for storing all epidemic data
            EpidemicDF = DataFrame()

            for mask_lvl in 0:mask_incr:99
                for vacc_lvl in 0:vacc_incr:99
                    # Check if Mask and Vax pair exists in MaskAndVaxDim, add if not
                    query = """
                        SELECT MaskVaxID FROM MaskVaxDim
                        WHERE MaskPortion = $mask_lvl 
                        AND VaxPortion = $vacc_lvl
                    """
                    Result = _run_query(query, connection) 
                    if size(Result)[1] == 0
                        query = """
                            INSERT INTO MaskVaxDim
                            SELECT
                                nextval('MaskVaxDimSequence') AS MaskVaxID,
                                $mask_lvl,
                                $vacc_lvl
                            RETURNING MaskVaxID
                        """
                        Result = _run_query(query, connection) 
                        MaskVaxID = Result[1,1]
                    else
                        MaskVaxID = Result[1,1]
                    end

                    # Populate BehaviorDim
                    BehaviorID = _store_behavior_dim_entry(NetworkdIDs[SocialNetworkIndex], MaskVaxID)

                    # Make a copy of the model
                    model_precontagion = deepcopy(ModelSocialNetwork)

                    # Apply masking to town
                    if distribution_type[1] == 0
                        mask_id_arr = Get_Portion_Random(model_precontagion, mask_lvl/100, [(x)->x.age >= 2])
                    elseif distribution_type[1] == 1
                        mask_id_arr = Get_Portion_Watts(model_precontagion, mask_lvl/100)
                    end
                    Update_Agents_Attribute!(model_precontagion, mask_id_arr, :will_mask, [true, true, true])

                    # Apply vaccination level to town
                    if distribution_type[2] == 0
                        vaccinated_id_arr = Get_Portion_Random(model_precontagion, vacc_lvl/100, [(x)-> x.age > 4 && x.age < 18, (x)->x.age >= 18], [0.34, 0.66])
                    elseif distribution_type[2] == 1
                        vaccinated_id_arr = Get_Portion_Watts(model_precontagion, vacc_lvl/100)
                    end
                    Update_Agents_Attribute!(model_precontagion, vaccinated_id_arr, :status, :V)
                    Update_Agents_Attribute!(model_precontagion, vaccinated_id_arr, :vaccinated, true)

                    # Collect and store each agents home and social behaviors
                    _store_agent_load(model_precontagion, BehaviorID)

                    # Build arrays for pmap
                    ModelContagionArr = [deepcopy(model_precontagion) for x in 1:runs]

                    # Seed and run model in parallel
                    ModelContagionArr = ModelContagionArr .|> Seed_Contagion!(model)

                    ModelRunsOutput = pmap(Run_Model!, ModelContagionArr)

                    # Gather output
                    TransmissionData = [x[3] for x in ModelRunsOutput]
                    SocialContactMatrices1 = [x[4] for x in ModelRunsOutput]
                    SummaryStatistics = [x[5] for x in ModelRunsOutput]

                    # Analyze the output
                    AgentDataArrayDaily = pmap(Get_Daily_Agentdata, [x[2] for x in ModelRunsOutput]) # Probably faster not pmapped

                    for epidemicIdx in 1:runs
                        EpidemicID = _store_epidemic_dim_entry(SummaryStatistics[epidemicIdx], BehaviorID)

                        # Populate EpidemicLoad
                        DuckDB.register_data_frame(connection, AgentDataArrayDaily[epidemicIdx], "AgentDataArrayDaily$(epidemicIdx)")
                        query = """
                            INSERT INTO EpidemicLoad
                            SELECT 
                                $EpidemicID,
                                *
                            FROM AgentDataArrayDaily$(epidemicIdx)
                        """
                        _run_query(query, connection)
                        _run_query("DROP VIEW AgentDataArrayDaily$(epidemicIdx)", connection)


                        # Populate TransmissionLoad
                        @show "Inserting Transmission Load"
                        DuckDB.register_data_frame(connection, TransmissionData[epidemicIdx], "TransmissionData$(epidemicIdx)")
                        query = """
                            INSERT INTO TransmissionLoad
                            SELECT 
                                $EpidemicID,
                                *
                            FROM TransmissionData$(epidemicIdx)
                        """
                        _run_query(query, connection)
                        _run_query("DROP VIEW TransmissionData$(epidemicIdx)", connection)

                        # Populate EpidemicSCMLoad
                        STORE_EPIDEMIC_SCM && _store_epidemic_scm(SocialContactMatrices1DF, EpidemicID)
                    end
                end
            end
            SocialNetworkIndex += 1
        end
    end
end

function _begin_simulations_faster(town_networks::Int, mask_levels::Int, vaccine_levels::Int, distribution_type::Vector{Int64}, runs::Int, duration_days_network, town, STORE_NETWORK_SCM::Bool, STORE_EPIDEMIC_SCM::Bool, number_workers::Int)
    # Ensure at least 2 threads are available, one main thread and one for the _dbWriterTask
    @assert Threads.nthreads() > 1 "Not enough threads to run multi-threaded simulation. $(Threads.nthreads()) detected, at least 2 required"

    # Build Workers
    println("Activating $(Base.active_project()) everywhere")
    addprocs(number_workers)
    eval(macroexpand(RuralABMDriver,quote @everywhere using Pkg end))
    @everywhere Pkg.instantiate()
    eval(macroexpand(RuralABMDriver,quote @everywhere using RuralABMDriver end))
    
    # Prepare town level channels
    townLevelWrites = 1
    networkLevelWrites = town_networks
    behaviorLevelWrites = mask_levels * vaccine_levels * town_networks
    epidemicLevelWrites = mask_levels * vaccine_levels * town_networks * runs
        
    # Prepare pipeline layers
    jobsChannel = RemoteChannel(()->Channel(2*number_workers))
    writesChannel = RemoteChannel(()->Channel(2*number_workers))

    populationModels = RemoteChannel(()->Channel(1)); 
    stableModels = RemoteChannel(()->Channel(town_networks));
    behavedModels = RemoteChannel(()->Channel(behaviorLevelWrites));

    townIdChannel = RemoteChannel(()->Channel(townLevelWrites))
    networkIdChannel = RemoteChannel(()->Channel(networkLevelWrites))
    behaviorIdChannel = RemoteChannel(()->Channel(behaviorLevelWrites))
    epidemicIdChannel = RemoteChannel(()->Channel(epidemicLevelWrites))

    println("All channels created")

    # On a separate thread begin the Writer() which handles all writes to the db
    writerTask = Threads.@spawn _dbWriterTask(townLevelWrites, networkLevelWrites, behaviorLevelWrites, epidemicLevelWrites, STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM, writesChannel, populationModels, stableModels, behavedModels, townIdChannel, networkIdChannel, behaviorIdChannel, epidemicIdChannel)

    # Run the raw models to establish a social network
    for p in workers()
        println("Spinning Up Worker $p")
        remote_do(Spin_Up_Worker, p, jobsChannel, writesChannel, duration_days_network)
    end

    # Generate the initial model object
    distribution_type[1] == 0 ? MaskDistributionType = "Random" : MaskDistributionType = "Watts"
    distribution_type[2] == 0 ? VaxDistributionType = "Random" : VaxDistributionType = "Watts"

    if town == "small" 
        PopulationID = 1
    elseif town == "large"
        PopulationID = 2
    end

    println("Constructing Town")
    model_raw , townDataSummaryDF, businessStructureDF, houseStructureDF = Construct_Town(joinpath("lib", "RuralABM", "data", "example_towns", "$(town)_town", "population.csv"), joinpath("lib", "RuralABM", "data", "example_towns", "$(town)_town", "businesses.csv"))
    model_raw.population_id = PopulationID
    model_raw.mask_distribution_type = MaskDistributionType
    model_raw.vax_distribution_type = VaxDistributionType
    model_raw.network_construction_length = duration_days_network
    put!(writesChannel, (model_raw, "Town Level"))
    
    # Store TownDim Level
    println("Feeding Town Structure")
    errormonitor(Threads.@spawn _populate_raw_model_channel(town_networks, jobsChannel, populationModels))    
    errormonitor(Threads.@spawn _populate_unbehaved_models(mask_levels, vaccine_levels, town_networks, jobsChannel, stableModels))
    errormonitor(Threads.@spawn _populate_seeded_models(runs, mask_levels, vaccine_levels, town_networks, jobsChannel, behavedModels))

    # Let dbWriterTask finish
    wait(writerTask)

    # Clean-up
    finalize(townIdChannel)
    finalize(networkIdChannel)
    finalize(behaviorIdChannel)
    finalize(epidemicIdChannel)

    # Kill wokrer processes
    rmprocs(workers()...)
end

function _dbWriterTask(townLevelWrites, networkLevelWrites, behaviorLevelWrites, epidemicLevelWrites, STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM, writesChannel, populationModels, stableModels, behavedModels, townIdChannel, networkIdChannel, behaviorIdChannel, epidemicIdChannel)
    connection = _create_default_connection()

    # Insert Id data
    query = """SELECT nextval('TownDimSequence')"""
    for _ in 1:townLevelWrites
       put!(townIdChannel, _run_query(query, connection)[1,1]) 
    end

    query = """SELECT nextval('NetworkDimSequence')"""
    for _ in 1:networkLevelWrites
        put!(networkIdChannel, _run_query(query, connection)[1,1]) 
    end

    query = """SELECT nextval('BehaviorDimSequence')"""
    for _ in 1:behaviorLevelWrites
        put!(behaviorIdChannel, _run_query(query, connection)[1,1]) 
    end

    query = """SELECT nextval('EpidemicDimSequence')"""
    for _ in 1:epidemicLevelWrites
        put!(epidemicIdChannel, _run_query(query, connection)[1,1]) 
    end
    
    jobs = townLevelWrites + networkLevelWrites + behaviorLevelWrites + epidemicLevelWrites
    @sync begin
        for i in 1:jobs
            model, task = take!(writesChannel)
    
            if task == "Town Level"
                errormonitor(Threads.@spawn _append_town_structure(connection, model, populationModels, townIdChannel))
            elseif task == "Network Level"
                errormonitor(Threads.@spawn _append_network_level_data(connection, model, STORE_NETWORK_SCM, stableModels, networkIdChannel))
            elseif task == "Behavior Level"
                errormonitor(Threads.@spawn _append_behavior_level_data(connection, model, behavedModels, behaviorIdChannel))
            elseif task == "Epidemic Level"
                errormonitor(Threads.@spawn _append_epidemic_level_data(connection, model, STORE_EPIDEMIC_SCM, epidemicIdChannel))
            end
            println("Jobs Complete: $i/$jobs")
            if (i % 200 == 0) 
                println("Garbage Collecting")
                @everywhere GC.gc()
            end
        end
    end 

    DBInterface.close(connection)
end

function _populate_seeded_models(runs, mask_levels, vaccine_levels, networks, jobsChannel, behavedModels)
    for _ in 1:(mask_levels * vaccine_levels * networks)
        behavedModel = take!(behavedModels)
        for _ in 1:runs
            put!(jobsChannel, (behavedModel, "Run Epidemic"))
        end
    end
end

function _populate_unbehaved_models(mask_levels, vaccine_levels, networks, jobsChannel, stableModels)
    # Compute target levels for masks and vaccines
    mask_incr = floor(100/(mask_levels))
    vacc_incr = floor(100/(vaccine_levels))

    for _ in 1:networks
        stableModel = take!(stableModels)
        for mask_lvl in 0:(mask_levels-1)
            for vacc_lvl in 0:(vaccine_levels-1)
                put!(jobsChannel, (stableModel, "Apply Behavior $(Int(mask_lvl*mask_incr)) $(Int(vacc_lvl*vacc_incr))"))
            end
        end
    end

end

function _populate_raw_model_channel(networks, jobsChannel, populationModels)
    model = take!(populationModels)

    for _ in 1:networks
        put!(jobsChannel, (model, "Build Network"))
    end
end

function _append_epidemic_level_data(connection, model, STORE_EPIDEMIC_SCM, epidemicIdChannel)

    model.epidemic_id = take!(epidemicIdChannel)

    # Append EpidemicDim data
    appender = DuckDB.Appender(connection, "EpidemicDim")
    DuckDB.append(appender, model.epidemic_id)
    DuckDB.append(appender, model.behavior_id)
    DuckDB.append(appender, model.epidemic_statistics[1,1])
    DuckDB.append(appender, model.epidemic_statistics[1,2])
    DuckDB.append(appender, model.epidemic_statistics[1,3])
    DuckDB.append(appender, model.epidemic_statistics[1,4])
    DuckDB.append(appender, model.epidemic_statistics[1,5])
    DuckDB.append(appender, model.epidemic_statistics[1,6])
    DuckDB.append(appender, model.epidemic_statistics[1,7])
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append EpidemicLoad data
    appender = DuckDB.Appender(connection, "EpidemicLoad")
    for row in eachrow(model.epidemic_data_daily)
        DuckDB.append(appender, model.epidemic_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Append TransmissionLoad data
    appender = DuckDB.Appender(connection, "TransmissionLoad")
    for row in eachrow(model.TransmissionNetwork)
        DuckDB.append(appender, model.epidemic_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Store Epidemic SCM
    if STORE_EPIDEMIC_SCM
        socialContactVector = Get_Compact_Adjacency_Matrix(model)
        appender = DuckDB.Appender(connection, "EpidemicSCMLoad")
        population = model.init_pop_size
        epidemicSCMItr = 1
        for agent1 in 1:population
            for agent2 in agent1+1:population
                DuckDB.append(appender, model.epidemic_id)
                DuckDB.append(appender, agent1)
                DuckDB.append(appender, agent2)
                DuckDB.append(appender, socialContactVector[epidemicSCMItr])
                DuckDB.end_row(appender)
                epidemicSCMItr += 1
            end
        end
        DuckDB.close(appender)
    end
end

function _append_behavior_level_data(connection, model, behavedModels, behaviorIdChannel)

    model.behavior_id = take!(behaviorIdChannel)
    put!(behavedModels, model)
    
    # Append to BehaviorDim
    appender = DuckDB.Appender(connection, "BehaviorDim")
    DuckDB.append(appender, model.behavior_id)
    DuckDB.append(appender, model.network_id)
    DuckDB.append(appender, model.mask_portion)
    DuckDB.append(appender, model.vax_portion)
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append to AgentLoad
    appender = DuckDB.Appender(connection, "AgentLoad")
    for agentId in 1:model.init_pop_size
        DuckDB.append(appender, model.behavior_id)
        DuckDB.append(appender, agentId)
        DuckDB.append(appender, model[agentId].home)
        DuckDB.append(appender, Int(model[agentId].will_mask[1]))
        DuckDB.append(appender, Int(model[agentId].vaccinated))
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)
end

function _append_town_structure(connection, model, populationModels, townIdChannel)
    model.town_id = take!(townIdChannel)
    put!(populationModels, model)

    # Populate TownDim
    appender = DuckDB.Appender(connection, "TownDim")
    DuckDB.append(appender, model.town_id)
    DuckDB.append(appender, model.population_id)
    DuckDB.append(appender, length(model.business))
    DuckDB.append(appender, length(model.houses))
    DuckDB.append(appender, length(model.school))
    DuckDB.append(appender, length(model.daycare))
    DuckDB.append(appender, length(model.community_gathering))
    DuckDB.append(appender, length(model.number_adults))
    DuckDB.append(appender, length(model.number_elders))
    DuckDB.append(appender, length(model.number_children))
    DuckDB.append(appender, length(model.number_empty_businesses))
    DuckDB.append(appender, model.mask_distribution_type)
    DuckDB.append(appender, model.vax_distribution_type)
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Populate BusinessLoad
    appender = DuckDB.Appender(connection, "BusinessLoad")
    for row in eachrow(model.business_structure_dataframe)
        DuckDB.append(appender, model.town_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Populate HouseholdLoad
    appender = DuckDB.Appender(connection, "HouseholdLoad")
    for row in eachrow(model.household_structure_dataframe)
        DuckDB.append(appender, model.town_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)
end

function _append_network_level_data(connection, model, STORE_NETWORK_SCM, stableModels, networkIdChannel)
    model.network_id = take!(networkIdChannel)
    put!(stableModels, model)    
    
    # Append NetworkDim data
    appender = DuckDB.Appender(connection, "NetworkDim")
    DuckDB.append(appender, model.network_id)
    DuckDB.append(appender, model.town_id)
    DuckDB.append(appender, model.network_construction_length)
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append NetworkSCMLoad data
    if STORE_NETWORK_SCM
        socialContactVector = Get_Compact_Adjacency_Matrix(model)
        appender = DuckDB.Appender(connection, "NetworkSCMLoad")

        population = model.init_pop_size
        epidemicSCMItr = 1
        for agent1 in 1:population
            for agent2 in agent1+1:population
                DuckDB.append(appender, model.epidemic_id)
                DuckDB.append(appender, agent1)
                DuckDB.append(appender, agent2)
                DuckDB.append(appender, socialContactVector[epidemicSCMItr])
                DuckDB.end_row(appender)
                epidemicSCMItr += 1
            end
        end
        DuckDB.close(appender)
    end
end

function _store_epidemic_scm(SocialContactMatrices1DF, epidemicID)
    connection = _create_default_connection()

    SocialContactMatrices1DF = DataFrame(SocialContactMatrices1, :auto)
    for SocialContactMatrix in eachcol(SocialContactMatrices1DF)
        EpidemicSCMAppender = DuckDB.Appender(connection, "EpidemicSCMLoad")
        
        Population = SocialContactMatrix[1] 
        EpidemicSCMItr = 2
        for agent1 in 1:Population
            for agent2 in agent1+1:Population
                DuckDB.append(EpidemicSCMAppender, epidemicID)
                DuckDB.append(EpidemicSCMAppender, agent1)
                DuckDB.append(EpidemicSCMAppender, agent2)
                DuckDB.append(EpidemicSCMAppender, SocialContactMatrix[EpidemicSCMItr])
                DuckDB.end_row(EpidemicSCMAppender)
                EpidemicSCMItr += 1
            end
        end
        DuckDB.close(EpidemicSCMAppender)
    end
end

function _store_agent_load(model, behaviorID)
    connection = _create_default_connection()
    AgentLoadAppender = DuckDB.Appender(connection, "AgentLoad")
    for agentID in 1:model.init_pop_size
        DuckDB.append(AgentLoadAppender, behaviorID)
        DuckDB.append(AgentLoadAppender, agentID)
        DuckDB.append(AgentLoadAppender, model[agentID].home)
        DuckDB.append(AgentLoadAppender, Int(model[agentID].will_mask[1]))
        DuckDB.append(AgentLoadAppender, Int(model[agentID].vaccinated))
        DuckDB.end_row(AgentLoadAppender)
    end
    DuckDB.close(AgentLoadAppender)
    disconnect_from_database!(connection)

    return true
end

function _store_epidemic_dim_entry(summaryStatistics, behaviorID)
    connection = _create_default_connection()

    query = """SELECT nextval('EpidemicDimSequence')"""
    result = _run_query(query, connection) 
    epidemicID = result[1,1]

    appender = DuckDB.Appender(connection, "EpidemicDim")
    DuckDB.append(appender, epidemicID)
    DuckDB.append(appender, behaviorID)
    DuckDB.append(appender, summaryStatistics[1,1])
    DuckDB.append(appender, summaryStatistics[1,2])
    DuckDB.append(appender, summaryStatistics[1,3])
    DuckDB.append(appender, summaryStatistics[1,4])
    DuckDB.append(appender, summaryStatistics[1,5])
    DuckDB.append(appender, summaryStatistics[1,6])
    DuckDB.append(appender, summaryStatistics[1,7])

    DuckDB.end_row(appender)
    DuckDB.close(appender)

    disconnect_from_database!(connection)

    return epidemicID
end

function _store_behavior_dim_entry(NetworkID, MaskVaxID)
    connection = _create_default_connection()
    query = """
        SELECT nextval('BehaviorDimSequence')
    """
    Result = _run_query(query, connection) 
    BehaviorID = Result[1,1]

    Appender = DuckDB.Appender(connection, "BehaviorDim")
    DuckDB.append(Appender, BehaviorID)
    DuckDB.append(Appender, NetworkID)
    DuckDB.append(Appender, MaskVaxID)
    DuckDB.end_row(Appender)
    DuckDB.close(Appender)

    disconnect_from_database!(connection)

    return BehaviorID
end