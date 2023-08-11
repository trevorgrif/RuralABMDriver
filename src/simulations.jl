
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
    STORE_EPIDEMIC_SCM = true
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

    # Establish connection to database and verify database structure (TODO: allow for database to be passed in)
    # connection = _create_default_connection()
    # @assert _verify_database_structure() "database structure is not valid"

    # Run simulations in parallel
    _begin_simulations_faster(SOCIAL_NETWORKS, MASKING_LEVELS, VACCINATION_LEVELS, DISTRIBUTION_TYPE, MODEL_RUNS, NETWORK_LENGTH, TOWN_NAMES, STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM)
    GC.gc()
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
        Result = _run_query(query, connection = connection) |> DataFrame
        TownID = Result[1,1]
        _run_query("DROP VIEW TownSummaryDF", connection = connection)


        # Insert business structure data
        DuckDB.register_data_frame(connection, businessStructureDF, "BusinessStructureDF")
        query = """
            INSERT INTO main.BusinessLoad
            SELECT
                $TownID,
                *,
            FROM BusinessStructureDF
        """
        _run_query(query, connection = connection)
        _run_query("DROP VIEW BusinessStructureDF", connection = connection)

        # Insert household structure data 
        DuckDB.register_data_frame(connection, houseStructureDF, "HouseStructureDF")
        query = """
            INSERT INTO main.HouseholdLoad
            SELECT
                $TownID,
                *,
            FROM HouseStructureDF
        """
        _run_query(query, connection = connection)
        _run_query("DROP VIEW HouseStructureDF", connection = connection)

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
            Results = _run_query(query, connection = connection) |> DataFrame
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
                    Result = _run_query(query, connection = connection) |> DataFrame
                    if size(Result)[1] == 0
                        query = """
                            INSERT INTO MaskVaxDim
                            SELECT
                                nextval('MaskVaxDimSequence') AS MaskVaxID,
                                $mask_lvl,
                                $vacc_lvl
                            RETURNING MaskVaxID
                        """
                        Result = _run_query(query, connection = connection) |> DataFrame
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
                        _run_query(query, connection = connection)
                        _run_query("DROP VIEW AgentDataArrayDaily$(epidemicIdx)", connection = connection)


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
                        _run_query(query, connection = connection)
                        _run_query("DROP VIEW TransmissionData$(epidemicIdx)", connection = connection)

                        # Populate EpidemicSCMLoad
                        STORE_EPIDEMIC_SCM && _store_epidemic_scm(SocialContactMatrices1DF, EpidemicID)
                    end
                end
            end
            SocialNetworkIndex += 1
        end
    end
end

function _begin_simulations_faster(town_networks::Int, mask_levels::Int, vaccine_levels::Int, distribution_type::Vector{Int64}, runs::Int, duration_days_network, town, STORE_NETWORK_SCM::Bool, STORE_EPIDEMIC_SCM::Bool)
    # Ensure at least 2 threads are available, one main thread and one for the _dbWriterTask
    @assert Threads.nthreads() > 1 "Not enough threads to run multi-threaded simulation. $(Threads.nthreads()) detected, at least 2 required"
    
    global jobsExists = true

    # Prepare town level channels
    townLevelWrites = 1
    global townLevelJobsChannel = Channel(1)
    global townLevelWritesChannel = Channel(1)
    put!(townLevelWritesChannel, townLevelWrites)
    
    # Prepare Channel for expected number of network level jobs and writes
    networkLevelJobs = town_networks
    networkLevelWrites = town_networks
    global networkLevelJobsChannel = RemoteChannel(()->Channel(1));
    global networkLevelWritesChannel = RemoteChannel(()->Channel(1));
    put!(networkLevelJobsChannel, networkLevelJobs)
    put!(networkLevelWritesChannel, networkLevelWrites)

    # Prepare Channel for expected number of behavior level jobs
    behaviorLevelJobs = mask_levels * vaccine_levels * town_networks
    behaviorLevelWrites = mask_levels * vaccine_levels * town_networks
    global behaviorLevelJobsChannel = RemoteChannel(()->Channel(1))
    global behaviorLevelWritesChannel = RemoteChannel(()->Channel(1))
    put!(behaviorLevelJobsChannel, behaviorLevelJobs)
    put!(behaviorLevelWritesChannel, behaviorLevelWrites)
    
    # Prepare Channel for expected number of behavior level jobs
    epidemicLevelJobs = mask_levels * vaccine_levels * town_networks * runs
    epidemicLevelWrites = mask_levels * vaccine_levels * town_networks * runs
    global epidemicLevelJobsChannel = RemoteChannel(()->Channel(1))
    global epidemicLevelWritesChannel = RemoteChannel(()->Channel(1))
    put!(epidemicLevelJobsChannel, epidemicLevelJobs)
    put!(epidemicLevelWritesChannel, epidemicLevelWrites)
    
    # Prepare pipeline layers
    global populationModels = RemoteChannel(()->Channel(1)); 

    global rawModels = RemoteChannel(()->Channel(32)); # Input for Run_Model! (pre-contagion)
    global stableModels = RemoteChannel(()->Channel(32)); 
    global networkLevelDataChannel = RemoteChannel(()->Channel(32)); 

    global misbehavedModels = RemoteChannel(()->Channel(mask_levels*vaccine_levels*town_networks)); # Input for Apply_Social_Behavior!
    global behavedModels = RemoteChannel(()->Channel(mask_levels*vaccine_levels*town_networks)); 
    global behaviorLevelDataChannel = RemoteChannel(()->Channel(mask_levels*vaccine_levels*town_networks));
    
    global seededModels = RemoteChannel(()->Channel(mask_levels*vaccine_levels*town_networks*runs)); # Input for Run_Model!
    global epidemicLevelDataChannel = RemoteChannel(()->Channel(mask_levels*vaccine_levels*town_networks*runs));

    # global progressBar = ProgressBar(total=(townLevelWrites + networkLevelWrites + behaviorLevelWrites +epidemicLevelWrites))


    # On a separate thread begin the Writer() which handles all writes to the db
    Threads.@spawn _dbWriterTask(STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM)

    distribution_type[1] == 0 ? MaskDistributionType = "Random" : MaskDistributionType = "Watts"
    distribution_type[2] == 0 ? VaxDistributionType = "Random" : VaxDistributionType = "Watts"

    if town == "small" 
        PopulationID = 1
    elseif town == "large"
        PopulationID = 2
    end

    # Generate the initial model object
    model_raw , townDataSummaryDF, businessStructureDF, houseStructureDF = Construct_Town("lib/RuralABM/data/example_towns/$(town)_town/population.csv", "lib/RuralABM/data/example_towns/$(town)_town/businesses.csv")
    model_raw.population_id = PopulationID
    model_raw.mask_distribution_type = MaskDistributionType
    model_raw.vax_distribution_type = VaxDistributionType
    model_raw.network_construction_length = duration_days_network
    
    # Store TownDim Level
    Threads.@spawn _feed_town_structure_channel(model_raw)
    Threads.@spawn _populate_raw_model_channel(town_networks)     

    # Run the raw models to establish a social network
    for p in workers()
        remote_do(Run_Model_Remote!, p, rawModels, networkLevelDataChannel, networkLevelJobsChannel, duration = duration_days_network)
    end
    
    # Multiply model in stableModels by vax_levels * max_levels
    Threads.@spawn _populate_unbehaved_models(mask_levels, vaccine_levels, town_networks)

    # Apply social behaviors
    for p in workers()
        remote_do(Apply_Social_Behavior!, p, misbehavedModels, behaviorLevelDataChannel, behaviorLevelJobsChannel)
    end
    Threads.@spawn _populate_seeded_models(runs, mask_levels, vaccine_levels, town_networks)

    # Run the seeded models
    for p in workers()
        remote_do(Run_Model_Remote!, p, seededModels, epidemicLevelDataChannel, epidemicLevelJobsChannel)
    end

    # Let dbWriterTask finish
    while jobsExists
        sleep(0.1) #temporarily unlock jobsExists
        fetch(townLevelWritesChannel) > 0 && continue
        fetch(networkLevelWritesChannel) > 0 && continue
        fetch(behaviorLevelWritesChannel) > 0 && continue
        fetch(epidemicLevelWritesChannel) > 0 && continue

        global jobsExists = false
    end
end

function _dbWriterTask(STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM)
    connection = _create_default_connection()
    while jobsExists
        sleep(0.1) #temporarily unlock jobsExists
        # update!(progressBar, 1)
        # set_multiline_postfix(progressBar, "Town Level Jobs Remaining: $(fetch(townLevelWritesChannel))\nNetwork Level Jobs Remaining: $(fetch(networkLevelWritesChannel))\nBehavior Level Jobs Remaining: $(fetch(behaviorLevelWritesChannel))\nEpidemic Level Jobs Remaining: $(fetch(epidemicLevelWritesChannel))")

        tasks = []
        isready(townLevelJobsChannel) && push!(tasks, Threads.@spawn _append_town_structure(connection))
        isready(networkLevelDataChannel) && push!(tasks, Threads.@spawn _append_network_level_data(connection, STORE_NETWORK_SCM))
        isready(behaviorLevelDataChannel) && push!(tasks, Threads.@spawn _append_behavior_level_data(connection))
        isready(epidemicLevelDataChannel) && push!(tasks, Threads.@spawn _append_epidemic_level_data(connection, STORE_EPIDEMIC_SCM))
        foreach(wait, tasks)
    end
    DuckDB.close(connection)
end

function _populate_seeded_models(runs, mask_levels, vaccine_levels, networks)
    behavedModelsRemaining = mask_levels * vaccine_levels * networks
    while behavedModelsRemaining > 0
        behavedModel = take!(behavedModels)
        jobCount = take!(behaviorLevelJobsChannel)
        put!(behaviorLevelJobsChannel, jobCount - 1)
        for _ in 1:runs
            model = deepcopy(behavedModel)
            Seed_Contagion!(model)
            put!(seededModels, deepcopy(model))
        end
        behavedModelsRemaining = behavedModelsRemaining - 1
    end
end

function _populate_unbehaved_models(mask_levels, vaccine_levels, networks)
    # Compute target levels for masks and vaccines
    mask_levels == 1 ? (mask_incr = 101) : (mask_incr = floor(100/(mask_levels-1)))
    vaccine_levels == 1 ? (vacc_incr = 101) : (vacc_incr = floor(100/(vaccine_levels-1)))

    networksRemaining = networks
    while true
        stableModel = take!(stableModels)
        jobCount = take!(networkLevelJobsChannel)
        put!(networkLevelJobsChannel, jobCount - 1)
        for mask_lvl in 0:mask_incr:100
            for vacc_lvl in 0:vacc_incr:100
                model = deepcopy(stableModel)
                model.mask_portion = mask_lvl
                model.vax_portion = vacc_lvl
                put!(misbehavedModels, deepcopy(model))
            end
        end
        networksRemaining = networksRemaining - 1
        networksRemaining == 0 && break
    end
end

function _populate_raw_model_channel(networks)
    while true
        model = take!(populationModels)
        for _ in 1:networks
            put!(rawModels, deepcopy(model))
        end
        break
    end
end


function _append_epidemic_level_data(connection, STORE_EPIDEMIC_SCM)
    model = take!(epidemicLevelDataChannel)

    query = """SELECT nextval('EpidemicDimSequence')"""
    epidemicId = _run_query(query, connection = connection) |> DataFrame
    epidemicId = epidemicId[1,1]
    model.epidemic_id = epidemicId

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

    model = 0
    jobCount = take!(epidemicLevelWritesChannel)
    put!(epidemicLevelWritesChannel, jobCount - 1)

end

function _append_behavior_level_data(connection)
    model = take!(behaviorLevelDataChannel)
    query = """SELECT nextval('BehaviorDimSequence')"""
    behaviorId = _run_query(query, connection = connection) |> DataFrame
    behaviorId = behaviorId[1,1]
    model.behavior_id = behaviorId
    put!(behavedModels, deepcopy(model))
    
    # Check MaskAndVaxDim
    query = """
    SELECT MaskVaxID FROM MaskVaxDim
    WHERE MaskPortion = $(model.mask_portion)
    AND VaxPortion = $(model.vax_portion)
    """
    result = _run_query(query, connection = connection) |> DataFrame
    if size(result)[1] == 0
        # Using "INSERT" since this case happens so rarely
        query = """ 
        INSERT INTO MaskVaxDim
        SELECT
        nextval('MaskVaxDimSequence') AS MaskVaxID,
        $(model.mask_portion),
        $(model.vax_portion)
        RETURNING MaskVaxID
        """
        result = _run_query(query, connection = connection) |> DataFrame
        maskVaxId = result[1,1]
    else
        maskVaxId = result[1,1]
    end
    
    # Append to BehaviorDim
    appender = DuckDB.Appender(connection, "BehaviorDim")
    DuckDB.append(appender, model.behavior_id)
    DuckDB.append(appender, model.network_id)
    DuckDB.append(appender, maskVaxId)
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

    model = 0
    jobCount = take!(behaviorLevelWritesChannel)
    put!(behaviorLevelWritesChannel, jobCount-1)

end

function _append_town_structure(connection)
    model = take!(townLevelJobsChannel)

    # Insert Town Structure Data
    query = """SELECT nextval('TownDimSequence')"""
    townId = _run_query(query, connection = connection) |> DataFrame
    townId = townId[1,1]
    model.town_id = townId
    put!(populationModels, deepcopy(model))


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
        DuckDB.append(appender, townId)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Populate HouseholdLoad
    appender = DuckDB.Appender(connection, "HouseholdLoad")
    for row in eachrow(model.household_structure_dataframe)
        DuckDB.append(appender, townId)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    model = 0

    jobCount = take!(townLevelWritesChannel)
    put!(townLevelWritesChannel, jobCount-1)

end

function _append_network_level_data(connection, STORE_NETWORK_SCM)
    model = take!(networkLevelDataChannel)

    query = """SELECT nextval('NetworkDimSequence')"""
    networkId = _run_query(query, connection = connection) |> DataFrame
    networkId = networkId[1,1]
    model.network_id = networkId
    
    put!(stableModels, deepcopy(model))    
    
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

    model = 0
    jobCount = take!(networkLevelWritesChannel)
    put!(networkLevelWritesChannel, jobCount-1)

end

function _feed_town_structure_channel(model)
    put!(townLevelJobsChannel, deepcopy(model))
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

    disconnect_from_database!(connection)
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
    result = _run_query(query, connection = connection) |> DataFrame
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
    Result = _run_query(query, connection = connection) |> DataFrame
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