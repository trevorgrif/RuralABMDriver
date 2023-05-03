
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
function run_ruralABM(;
    SOCIAL_NETWORKS::Int = 10,
    NETWORK_LENGTH::Int = 30,
    MASKING_LEVELS::Int = 5,
    VACCINATION_LEVELS::Int = 5,
    DISTRIBUTION_TYPE = [0, 0],
    MODEL_RUNS::Int = 100,
    TOWN_NAMES = ["small"]
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
    connection = create_default_connection()
    @assert verify_database_structure() "database structure is not valid"

    # Run simulations in parallel
    begin_simulations(SOCIAL_NETWORKS, MASKING_LEVELS, VACCINATION_LEVELS, DISTRIBUTION_TYPE, MODEL_RUNS, NETWORK_LENGTH, TOWN_NAMES)
end

"""
    begin_simulations(town_networks:, mask_levels, vaccine_levels, runs, duration_days_network, towns)

Run RuralABM simulations based on the values passed. See documentation of Run_RuralABM for details.
"""
function begin_simulations(town_networks::Int, mask_levels::Int, vaccine_levels::Int, distribution_type::Vector{Int64}, runs::Int, duration_days_network, towns; STORE_NETWORK_SCM::Bool = true, STORE_EPIDEMIC_SCM::Bool = true)
    # Compute target levels for masks and vaccines
    mask_incr = floor(100/mask_levels)
    vacc_incr = floor(100/vaccine_levels)

    connection = create_default_connection()

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
        Result = run_query(query, connection = connection) |> DataFrame
        TownID = Result[1,1]
        run_query("DROP VIEW TownSummaryDF", connection = connection)


        # Insert business structure data
        DuckDB.register_data_frame(connection, businessStructureDF, "BusinessStructureDF")
        query = """
            INSERT INTO main.BusinessLoad
            SELECT
                $TownID,
                *,
            FROM BusinessStructureDF
        """
        run_query(query, connection = connection)
        run_query("DROP VIEW BusinessStructureDF", connection = connection)

        # Insert household structure data 
        DuckDB.register_data_frame(connection, houseStructureDF, "HouseStructureDF")
        query = """
            INSERT INTO main.HouseholdLoad
            SELECT
                $TownID,
                *,
            FROM HouseStructureDF
        """
        run_query(query, connection = connection)
        run_query("DROP VIEW HouseStructureDF", connection = connection)

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
            Results = run_query(query, connection = connection) |> DataFrame
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

        # Forced Garbage Collection
        ResultsPostSocialNetworks = 0
        SocialContactMatrices0 = 0
        SocialContactMatrices0DF = 0
        GC.gc()
        
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
                    Result = run_query(query, connection = connection) |> DataFrame
                    if size(Result)[1] == 0
                        query = """
                            INSERT INTO MaskVaxDim
                            SELECT
                                nextval('MaskVaxDimSequence') AS MaskVaxID,
                                $mask_lvl,
                                $vacc_lvl
                            RETURNING MaskVaxID
                        """
                        Result = run_query(query, connection = connection) |> DataFrame
                        MaskVaxID = Result[1,1]
                    else
                        MaskVaxID = Result[1,1]
                    end

                    # Populate BehaviorDim
                    query = """
                        INSERT INTO BehaviorDim
                        SELECT
                            nextval('BehaviorDimSequence') AS BehaviorID,
                            $(NetworkdIDs[SocialNetworkIndex]),
                            $MaskVaxID
                        RETURNING BehaviorID
                    """
                    Result = run_query(query, connection = connection) |> DataFrame
                    BehaviorID = Result[1,1]

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
                    AgentLoadAppender = DuckDB.Appender(connection, "AgentLoad")
                    for agentID in 1:model_precontagion.init_pop_size
                        DuckDB.append(AgentLoadAppender, BehaviorID)
                        DuckDB.append(AgentLoadAppender, agentID)
                        DuckDB.append(AgentLoadAppender, model_precontagion[agentID].home)
                        DuckDB.append(AgentLoadAppender, Int(model_precontagion[agentID].will_mask[1]))
                        DuckDB.append(AgentLoadAppender, Int(model_precontagion[agentID].vaccinated))
                        DuckDB.end_row(AgentLoadAppender)
                    end
                    DuckDB.close(AgentLoadAppender)

                    # Build arrays for pmap
                    ModelContagionArr = [deepcopy(model_precontagion) for x in 1:runs]

                    # Seed and run model in parallel
                    for model in ModelContagionArr
                        Seed_Contagion!(model)
                    end
                    ModelRunsOutput = pmap(Run_Model!, ModelContagionArr)

                    # Gather output
                    TransmissionData = [x[3] for x in ModelRunsOutput]
                    SocialContactMatrices1 = [x[4] for x in ModelRunsOutput]
                    SummaryStatistics = [x[5] for x in ModelRunsOutput]

                    # Analyze the output
                    AgentDataArrayDaily = pmap(Get_Daily_Agentdata, [x[2] for x in ModelRunsOutput]) # Probably faster not pmapped
                    SocialContactMatrices1DF = DataFrame(SocialContactMatrices1, :auto) # Probably better to leave as vector

                    EpidemicIDs = []
                    for epidemicIdx in 1:runs
                        # Populate EpidemicDim
                        query = """
                            INSERT INTO EpidemicDim
                            SELECT
                                nextval('EpidemicDimSequence') AS EpidemicID,
                                $BehaviorID,
                                $(SummaryStatistics[epidemicIdx][1,1]),
                                $(SummaryStatistics[epidemicIdx][1,2]),
                                $(SummaryStatistics[epidemicIdx][1,3]),
                                $(SummaryStatistics[epidemicIdx][1,4]),
                                $(SummaryStatistics[epidemicIdx][1,5]),
                                $(SummaryStatistics[epidemicIdx][1,6]),
                                $(SummaryStatistics[epidemicIdx][1,7])
                            RETURNING EpidemicID
                        """
                        Result = run_query(query, connection = connection) |> DataFrame
                        EpidemicID = Result[1,1]
                        append!(EpidemicIDs, EpidemicID)

                        # Populate EpidemicLoad
                        DuckDB.register_data_frame(connection, AgentDataArrayDaily[epidemicIdx], "AgentDataArrayDaily$(epidemicIdx)")
                        query = """
                            INSERT INTO EpidemicLoad
                            SELECT 
                                $EpidemicID,
                                *
                            FROM AgentDataArrayDaily$(epidemicIdx)
                        """
                        run_query(query, connection = connection)
                        run_query("DROP VIEW AgentDataArrayDaily$(epidemicIdx)", connection = connection)


                        # Populate TransmissionLoad
                        DuckDB.register_data_frame(connection, TransmissionData[epidemicIdx], "TransmissionData$(epidemicIdx)")
                        query = """
                            INSERT INTO TransmissionLoad
                            SELECT 
                                $EpidemicID,
                                *
                            FROM TransmissionData$(epidemicIdx)
                        """
                        run_query(query, connection = connection)
                        run_query("DROP VIEW TransmissionData$(epidemicIdx)", connection = connection)
                    end

                    # Populate EpidemicSCMLoad
                    if STORE_EPIDEMIC_SCM
                        isdir("data/EpidemicSCMLoad/") || mkdir("data/EpidemicSCMLoad/")
                        insert!(SocialContactMatrices1DF, 1, EpidemicIDs)
                        write_parquet("data/EpidemicSCMLoad/$(BehaviorID).parquet", SocialContactMatrices1DF)

                        # Create view into parquet file
                        query = """
                            CREATE VIEW EpidemicSCMLoad_$BehaviorID AS
                                SELECT * 
                                FROM 'data/EpidemicSCMLoad/$(BehaviorID).parquet';
                        """
                        run_query(query, connection = connection)
                    end

                    # Forced Garbage Collection
                    model_precontagion = 0
                    mask_id_arr = 0
                    vaccinated_id_arr = 0
                    ModelContagionArr = 0
                    ModelRunsOutput = 0
                    SocialContactMatrices1 = 0
                    SocialContactMatrices1DF = 0
                    SummaryStatistics = 0
                    AgentDataArrayDaily = 0
                    GC.gc()
                end
            end
            SocialNetworkIndex += 1

            # Forced Garbage Collection
            EpidemicDF = 0
            GC.gc()
        end
    end
end