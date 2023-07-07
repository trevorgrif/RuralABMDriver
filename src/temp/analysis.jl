using PlotlyJS
using Graphs
using Base.Threads: @spawn

using Random
using GLM
using StatsBase
using LinearAlgebra

include("import.jl")

#######################################
#   Age Structured Contact Matrices   #
#######################################

function plot_age_structured_contact(NetworkID, connection)
    data = compute_age_structured_contact_matrix(NetworkID, connection)
    data = unstack(data, :ContactGroup, :Weight)
    # data = data_unstacked[1:end,2:end]

    data = select(data, 
        AsTable(Cols("00-04")) => sum, 
        AsTable(Cols("05-09")) => sum, 
        AsTable(Cols("10-14")) => sum, 
        AsTable(Cols("15-17","18-19")) => sum, 
        AsTable(Cols("20-20","21-21","22-24","25-29")) => sum, 
        AsTable(Cols("30-34","35-39")) => sum, 
        AsTable(Cols("40-44","45-49")) => sum, 
        AsTable(Cols("50-54","55-59")) => sum, 
        AsTable(Cols("60-61","62-64","65-66","67-69")) => sum, 
        AsTable(Cols("70-74","75-79","80-84","85-NA")) => sum
        )

    col1 = sum.(eachcol(data[1:1,1:end]))
    col2 = sum.(eachcol(data[2:2,1:end]))
    col3 = sum.(eachcol(data[3:3,1:end]))
    col4 = sum.(eachcol(data[4:5,1:end]))
    col5 = sum.(eachcol(data[6:9,1:end]))
    col6 = sum.(eachcol(data[10:11,1:end]))
    col7 = sum.(eachcol(data[12:13,1:end]))
    col8 = sum.(eachcol(data[14:15,1:end]))
    col9 = sum.(eachcol(data[16:19,1:end]))
    col10 = sum.(eachcol(data[20:end,1:end]))

    data = DataFrame(
        A = col1,
        B = col2,
        C = col3,
        D = col4,
        E = col5,
        F = col6,
        G = col7,
        H = col8,
        I = col9,
        J = col10
        )
    
    AgeData = [
        "0 to 4",
        "5 to 9",
        "10 to 14",
        "15 to 19",
        "20 to 29",
        "30 to 39", 
        "40 to 49", 
        "50 to 59", 
        "60 to 69", 
        "70+"
        ]

    @show data
    plot(
        heatmap(
            x = AgeData,
            y = AgeData,
            z = Matrix(data),
        ),
        Layout(
            xaxis_title="Age",
            yaxis_title="Age of Contact",
            title="Age Structured Contacts: Network $NetworkID"
        )
    )
end

function age_structured_contact(PopulationID, con)
    query = """
    SELECT AgeRangeID, string_agg(AgentID, ',')  AS AgentList
    FROM PopulationLoad 
    WHERE PopulationID = $PopulationID
    GROUP BY AgeRangeID
    ORDER BY AgeRangeID
    """
    return DataFrame(run_query(query,con))
end

function cross_age_range_contacts(AgentIDs1::Vector{Int64}, AgentIDs2::Vector{Int64}, NetworkID, con)
    query = """
    SELECT SUM(Weight) AS TotalContacts 
    FROM NetworkSCMLoad 
    WHERE (NetworkID = $NetworkID 
    AND Agent1 IN ($(string(AgentIDs1)[2:end-1])) 
    AND Agent2 IN ($(string(AgentIDs2)[2:end-1]))
    )
    OR (NetworkID = $NetworkID
    AND Agent2 IN ($(string(AgentIDs1)[2:end-1])) 
    AND Agent1 IN ($(string(AgentIDs2)[2:end-1]))
    )
    """
    result = DataFrame(run_query(query, con))[1,1]
    
    if typeof(result) == Missing
        return 0
    end
    return result 
end

function compute_age_structured_contact_matrix(NetworkID, connection)
    AgeRangeDF = age_structured_contact(1,connection)

    Data::Matrix{Int64} = zeros(size(AgeRangeDF,1), size(AgeRangeDF,1))
    AgeRangeItr = 1
    DataDF = DataFrame(AgeGroup = String[], ContactGroup = String[], Weight = Int64[])
    for AgeRange in eachrow(AgeRangeDF)
        SubRangeItr = 1
        for SubRange in eachrow(AgeRangeDF)
            # Compute the contacts AgeRange have with SubRange
            AgeRangeVector::Vector{Int64} = convert_to_vector(AgeRange[2])
            SubRangeVector::Vector{Int64} = convert_to_vector(SubRange[2]) 
            Weight = cross_age_range_contacts(AgeRangeVector, SubRangeVector, NetworkID, connection)
            Data[AgeRangeItr, SubRangeItr] = Weight
            SubRangeItr += 1
            append!(DataDF, DataFrame(AgeGroup = [AgeRange[1]], ContactGroup = [SubRange[1]], Weight = [Weight]))
        end 
        AgeRangeItr += 1
    end
    return DataDF
    
    return data
end

function convert_to_vector(List)
    return parse.(Int64, split(List, ","))
end

############################
#    Summary Statistics    #
############################

function ratio_infection_deaths(connection)
    query = """
        SELECT MaskPortion, VaxPortion, AVG(CAST(InfectedTotal AS DECIMAL) / (386 - InfectedTotal + RecoveredTotal)) AS RatioInfectionDeaths 
        FROM EpidemicDim
        GROUP BY MaskPortion, VaxPortion 
    """
    run_query(query, connection)
end

function ComputeSummaryStats(Population, con)

    OutbreakThreshold = 0.1*Population

    query = """
    WITH MaskedAndVaxedAgents AS (
        SELECT 
            AgentLoad.BehaviorID,
            AgentLoad.AgentID
        FROM AgentLoad
        WHERE IsMasking = 1
        AND IsVaxed = 1
    ),
    InfectedAndProtectedAgents AS (
        SELECT 
            NetworkDim.NetworkID,
            BehaviorDim.BehaviorID,
            EpidemicDim.EpidemicID,
            COUNT(TransmissionLoad.AgentID) AS ProtectedAndInfectedCount
        FROM BehaviorDim
        JOIN EpidemicDim
        ON EpidemicDim.BehaviorID = BehaviorDim.BehaviorID
        JOIN TransmissionLoad
        ON TransmissionLoad.EpidemicID = EpidemicDim.EpidemicID
        JOIN NetworkDim
        ON NetworkDim.NetworkID = BehaviorDim.NetworkID
        WHERE TransmissionLoad.AgentID IN ( 
            SELECT AgentID 
            FROM MaskedAndVaxedAgents
            WHERE MaskedAndVaxedAgents.BehaviorID = BehaviorDim.BehaviorID
            )
        AND InfectedTotal > $(OutbreakThreshold)
        GROUP BY  NetworkDim.NetworkID, BehaviorDim.BehaviorID, EpidemicDim.EpidemicID
    ),
    AggregateInfectedAndProtectedCount AS (
        SELECT 
            InfectedAndProtectedAgents.NetworkID,
            InfectedAndProtectedAgents.BehaviorID,
            AVG(ProtectedAndInfectedCount) AS AverageMaskedVaxedInfectedCount
        FROM InfectedAndProtectedAgents
        GROUP BY InfectedAndProtectedAgents.BehaviorID, InfectedAndProtectedAgents.NetworkID
    ),
    OutbreakSupressionCounts AS (
        SELECT 
            BehaviorID,
           SUM(CASE WHEN InfectedTotal <= $OutbreakThreshold THEN 1 ELSE 0 END) AS OutbreakSuppresionCount,
           SUM(CASE WHEN InfectedTotal > $OutbreakThreshold THEN 1 ELSE 0 END) AS OutbreakCount
        FROM EpidemicDim
        GROUP BY BehaviorID
    ),
    MaskVaxCounts AS (
        SELECT 
            BehaviorID,
            SUM(IsMasking) AS IsMaskingCount,
            SUM(IsVaxed) AS IsVaxedCount,
            SUM(CASE WHEN IsVaxed = 1 THEN IsMasking ELSE 0 END) AS IsMaskingAndVaxed
        FROM AgentLoad
        GROUP BY BehaviorID
    ),
    TownData AS (
        SELECT 
            lpad(BehaviorDim.NetworkID, 2, '0') AS NetworkID, 
            TownDim.MaskDistributionType,
            TownDim.VaxDistributionType,
            MaskVaxDim.MaskPortion, 
            MaskVaxDim.VaxPortion,
            IsMaskingCount,
            IsVaxedCount,
            IsMaskingAndVaxed AS IsMaskingVaxedCount,
            OutbreakSupressionCounts.OutbreakSuppresionCount,
            OutbreakSupressionCounts.OutbreakCount,
            CASE WHEN AverageMaskedVaxedInfectedCount IS NULL THEN 0 ELSE AverageMaskedVaxedInfectedCount END AS AverageMaskedVaxedInfectedCount,
            InfectedTotal,
            InfectedMax,
            PeakDay,
            RecoveredTotal
        FROM TownDim
        JOIN NetworkDim
        ON NetworkDim.TownID = TownDim.TownID
        JOIN BehaviorDim        
        ON NetworkDim.NetworkID = BehaviorDim.NetworkID
        JOIN MaskVaxDim
        ON BehaviorDim.MaskVaxID = MaskVaxDim.MaskVaxID
        JOIN MaskVaxCounts
        ON MaskVaxCounts.BehaviorID = BehaviorDim.BehaviorID
        JOIN EpidemicDim
        ON BehaviorDim.BehaviorID = EpidemicDim.BehaviorID
        JOIN OutbreakSupressionCounts
        ON OutbreakSupressionCounts.BehaviorID = BehaviorDim.BehaviorID
        LEFT JOIN AggregateInfectedAndProtectedCount
        ON AggregateInfectedAndProtectedCount.BehaviorID = BehaviorDim.BehaviorID
    )
    SELECT 
        NetworkID, 
        MaskDistributionType,
        VaxDistributionType,
        MaskPortion, 
        VaxPortion,
        IsMaskingCount,
        IsVaxedCount,
        IsMaskingVaxedCount,
        OutbreakSuppresionCount,
        OutbreakCount,
        CAST(OutbreakCount AS DECIMAL)/(OutbreakCount+OutbreakSuppresionCount) AS ProbabilityOfOutbreak,
        AverageMaskedVaxedInfectedCount,
        AverageMaskedVaxedInfectedCount/IsMaskingVaxedCount AS ProbabilityOfInfectionWhenProtected,
        AVG(InfectedTotal) AS AverageInfectedTotal, 
        AVG(InfectedTotal)/$Population AS AverageInfectedPercentage,
        var_samp(InfectedTotal) AS VarianceInfectedTotal, 
        AVG(InfectedMax) AS AverageInfectedMax, 
        var_samp(InfectedMax) AS VarianceInfectedMax,
        AVG(PeakDay) AS AveragePeakDay, 
        var_samp(PeakDay)  As VariancePeakDay,
        AVG(CAST(InfectedTotal AS DECIMAL) / ($Population - InfectedTotal + RecoveredTotal)) AS RatioInfectionDeaths 
    FROM TownData
    WHERE InfectedTotal > $(OutbreakThreshold)
    GROUP BY NetworkID, MaskPortion, VaxPortion, MaskDistributionType, VaxDistributionType, IsMaskingCount, IsVaxedCount, IsMaskingVaxedCount, AverageMaskedVaxedInfectedCount, ProbabilityOfInfectionWhenProtected, OutbreakSuppresionCount, OutbreakCount
    ORDER BY MaskPortion, VaxPortion, NetworkID
    """
    #run_query(query, con)
    CSV.write("StatsDF.csv", run_query(query, con) |> DataFrame)
end

function Compute_Global_Clustering_Coefficient(connection)
    # Iterate over Network SCM
    NetworkIDs = [1,2,3,4,5,6,7,8,9,10]
    GlobalClusteringCoefficients = []
    for NetworkID in NetworkIDs
        # Extract Network Data into Graphs.jl Graph object
        query = """
        SELECT Agent1, Agent2, Weight
        FROM NetworkSCMLoad
        WHERE NetworkID = $NetworkID
        """
        NetworkSCMLoad = run_query(query, connection) |> DataFrame
        

        # Compute global clustering coefficient
        GlobalClusteringCoefficient =  global_clustering_coefficient(NetworkSCM)

        # Load into array of results
        append!(GlobalClusteringCoefficients, GlobalClusteringCoefficient)
    end

    return GlobalClusteringCoefficients
end

"""
Plot the population by age range as a bar graph
"""
function plot_population_distribution(PopulationID, connection)
    query = """
    SELECT 
        replace(AgeRangeID, '-', ' to ')AS AgeRangeID, 
        COUNT(*) AS BinSize 
    FROM PopulationLoad 
    WHERE PopulationID = $PopulationID 
    GROUP BY AgeRangeID 
    ORDER BY AgeRangeID
    """
    data = run_query(query, connection) |> DataFrame

    col1 = sum.(eachcol(data[1:1,2:end]))
    col2 = sum.(eachcol(data[2:2,2:end]))
    col3 = sum.(eachcol(data[3:3,2:end]))
    col4 = sum.(eachcol(data[4:5,2:end]))
    col5 = sum.(eachcol(data[6:9,2:end]))
    col6 = sum.(eachcol(data[10:11,2:end]))
    col7 = sum.(eachcol(data[12:13,2:end]))
    col8 = sum.(eachcol(data[14:15,2:end]))
    col9 = sum.(eachcol(data[16:19,2:end]))
    col10 = sum.(eachcol(data[20:end,2:end]))

    data = DataFrame(AgeRangeID = String[], BinSize = Int64[])
    append!(data, DataFrame(AgeRangeID = "00 to 04", BinSize = col1[1]))
    append!(data, DataFrame(AgeRangeID = "05 to 09", BinSize = col2[1]))
    append!(data, DataFrame(AgeRangeID = "10 to 14", BinSize = col3[1]))
    append!(data, DataFrame(AgeRangeID = "15 to 19", BinSize = col4[1]))
    append!(data, DataFrame(AgeRangeID = "20 to 29", BinSize = col5[1]))
    append!(data, DataFrame(AgeRangeID = "30 to 39", BinSize = col6[1]))
    append!(data, DataFrame(AgeRangeID = "40 to 49", BinSize = col7[1]))
    append!(data, DataFrame(AgeRangeID = "50 to 59", BinSize = col8[1]))
    append!(data, DataFrame(AgeRangeID = "60 to 69", BinSize = col9[1]))
    append!(data, DataFrame(AgeRangeID = "70+", BinSize = col10[1]))

    @show data

    plot(
        data,
        x = :AgeRangeID,
        y = :BinSize,
        kind = "bar"
    )

end

function logistic_regression_thickness(data; VaxLevels = [], DistributionTypes = [])
    # Variable setting
    TargetMaskVaxIDs = []
    TargetNetworkIDs = []

    0 in VaxLevels && append!(TargetMaskVaxIDs, [1,6,11,16,21])
    20 in VaxLevels && append!(TargetMaskVaxIDs, [2,7,12,17,22])
    40 in VaxLevels && append!(TargetMaskVaxIDs, [3,8,13,18,23])
    60 in VaxLevels && append!(TargetMaskVaxIDs, [4,9,14,19,24])
    80 in VaxLevels && append!(TargetMaskVaxIDs, [5,10,15,20,25])

    # 1 and 3 are random, 2 and 3 are watts on vax
    1 in DistributionTypes && append!(TargetNetworkIDs, [1,2,3,4,5,6,7,8,9,10])
    2 in DistributionTypes && append!(TargetNetworkIDs, [11,12,13,14,15,16,17,18,19,20])
    3 in DistributionTypes && append!(TargetNetworkIDs, [21,22,23,24,25,26,27,28,29,30])
    4 in DistributionTypes && append!(TargetNetworkIDs, [31,32,33,34,35,36,37,38,39,40])

    # select relevant data
    data = data[in(TargetNetworkIDs).(data.NetworkID), :]
    data = data[in(TargetMaskVaxIDs).(data.MaskVaxID), :]
    select!(data, Not([:NetworkID, :BehaviorID, :EpidemicID, :MaskVaxID]))

    # Count the number of outbreaks
    Outbreaks = data[data.Outbreak .== 1, :]
    Suppressions = data[data.Outbreak .== 0, :]
    MaxClassCount = min(nrow(Outbreaks), nrow(Suppressions))
    @show nrow(Outbreaks)
    @show nrow(Suppressions)

    Outbreaks = Outbreaks[shuffle(1:nrow(Outbreaks))[1:MaxClassCount], :]
    Suppressions = Suppressions[shuffle(1:nrow(Suppressions))[1:MaxClassCount], :]

    data = append!(Suppressions, Outbreaks)
    data = Suppressions[shuffle(1:nrow(data)), :]
    
    # split the data
    train = first(data, Int(floor(0.75*nrow(data))))
    test = last(data, Int(floor(0.25*nrow(data))))

    # Create and train model
    fm = @formula(Outbreak ~ AverageThickness + PopulationVarianceThickness)
    logit = glm(fm, train, Binomial(), LogitLink())

    # Predict the target variable on test data 
    prediction = predict(logit, test)

    # Convert probability score to class
    prediction_class = [if x < 0.5 0 else 1 end for x in prediction];

    prediction_df = DataFrame(y_actual = test.Outbreak, y_predicted = prediction_class, prob_predicted = prediction);
    prediction_df.correctly_classified = prediction_df.y_actual .== prediction_df.y_predicted

    # Accuracy Score
    accuracy = mean(prediction_df.correctly_classified)
    print("Accuracy of the model is : ", accuracy)
end

function plot_thickness(connection)
    for i in 1:10:31
        thick = compute_thickness(i, connection)
        trace =  scatter(
            thick,
            x=:dist,
            y=:tau,
            mode="lines"
            )
        layout = Layout(
            xaxis_title="Epsilon",
            yaxis_title="Thickness",
            title="Thickness over Epsilons: Network $i"
            )
        # savefig(
        savefig(plot(trace, layout),
            "Thickness_$(lpad(i,2,"0")).png")
    end
end

function plot_thickness_mean_variance(data; VaxLevels = [], DistributionTypes = [])
    # Variable setting
    TargetMaskVaxIDs = []
    TargetNetworkIDs = []

    0 in VaxLevels && append!(TargetMaskVaxIDs, [1,6,11,16,21])
    20 in VaxLevels && append!(TargetMaskVaxIDs, [2,7,12,17,22])
    40 in VaxLevels && append!(TargetMaskVaxIDs, [3,8,13,18,23])
    60 in VaxLevels && append!(TargetMaskVaxIDs, [4,9,14,19,24])
    80 in VaxLevels && append!(TargetMaskVaxIDs, [5,10,15,20,25])

    # 1 and 3 are random, 2 and 3 are watts on vax
    1 in DistributionTypes && append!(TargetNetworkIDs, [1,2,3,4,5,6,7,8,9,10])
    2 in DistributionTypes && append!(TargetNetworkIDs, [11,12,13,14,15,16,17,18,19,20])
    3 in DistributionTypes && append!(TargetNetworkIDs, [21,22,23,24,25,26,27,28,29,30])
    4 in DistributionTypes && append!(TargetNetworkIDs, [31,32,33,34,35,36,37,38,39,40])
    5 in DistributionTypes && append!(TargetNetworkIDs, [41,42,43,44,45,46,47,48,49,50])

    # select relevant data
    data = data[in(TargetNetworkIDs).(data.NetworkID), :]
    data = data[in(TargetMaskVaxIDs).(data.MaskVaxID), :]
    data = data[in([-1,0,1]).(data.Outbreak), :]
    select!(data, Not([:NetworkID, :BehaviorID, :EpidemicID, :MaskVaxID]))
    plot(data, y=:AverageThickness, x=:PopulationVarianceThickness, color=:Outbreak , mode="markers")
end

function plot_infected_total(data; VaxLevels = [], DistributionTypes = [])
    # Variable setting
    TargetMaskVaxIDs = []
    TargetNetworkIDs = []

    0 in VaxLevels && append!(TargetMaskVaxIDs, [1,6,11,16,21])
    20 in VaxLevels && append!(TargetMaskVaxIDs, [2,7,12,17,22])
    40 in VaxLevels && append!(TargetMaskVaxIDs, [3,8,13,18,23])
    60 in VaxLevels && append!(TargetMaskVaxIDs, [4,9,14,19,24])
    80 in VaxLevels && append!(TargetMaskVaxIDs, [5,10,15,20,25])

    # 1 and 3 are random, 2 and 3 are watts on vax
    1 in DistributionTypes && append!(TargetNetworkIDs, [1,2,3,4,5,6,7,8,9,10])
    2 in DistributionTypes && append!(TargetNetworkIDs, [11,12,13,14,15,16,17,18,19,20])
    3 in DistributionTypes && append!(TargetNetworkIDs, [21,22,23,24,25,26,27,28,29,30])
    4 in DistributionTypes && append!(TargetNetworkIDs, [31,32,33,34,35,36,37,38,39,40])

    # select relevant data
    data = data[in(TargetNetworkIDs).(data.NetworkID), :]
    data = data[in(TargetMaskVaxIDs).(data.MaskVaxID), :]

    histogram(data.InfectedTotal)
end

function plot_infected_max(data; VaxLevels = [], DistributionTypes = [])
    # Variable setting
    TargetMaskVaxIDs = []
    TargetNetworkIDs = []

    0 in VaxLevels && append!(TargetMaskVaxIDs, [1,6,11,16,21])
    20 in VaxLevels && append!(TargetMaskVaxIDs, [2,7,12,17,22])
    40 in VaxLevels && append!(TargetMaskVaxIDs, [3,8,13,18,23])
    60 in VaxLevels && append!(TargetMaskVaxIDs, [4,9,14,19,24])
    80 in VaxLevels && append!(TargetMaskVaxIDs, [5,10,15,20,25])

    # 1 and 3 are random, 2 and 3 are watts on vax
    1 in DistributionTypes && append!(TargetNetworkIDs, [1,2,3,4,5,6,7,8,9,10])
    2 in DistributionTypes && append!(TargetNetworkIDs, [11,12,13,14,15,16,17,18,19,20])
    3 in DistributionTypes && append!(TargetNetworkIDs, [21,22,23,24,25,26,27,28,29,30])
    4 in DistributionTypes && append!(TargetNetworkIDs, [31,32,33,34,35,36,37,38,39,40])

    # select relevant data
    data = data[in(TargetNetworkIDs).(data.NetworkID), :]
    data = data[in(TargetMaskVaxIDs).(data.MaskVaxID), :]

    histogram(data.InfectedMax)
end

function plot_peak_day(data; VaxLevels = [], DistributionTypes = [])
    # Variable setting
    TargetMaskVaxIDs = []
    TargetNetworkIDs = []

    0 in VaxLevels && append!(TargetMaskVaxIDs, [1,6,11,16,21])
    20 in VaxLevels && append!(TargetMaskVaxIDs, [2,7,12,17,22])
    40 in VaxLevels && append!(TargetMaskVaxIDs, [3,8,13,18,23])
    60 in VaxLevels && append!(TargetMaskVaxIDs, [4,9,14,19,24])
    80 in VaxLevels && append!(TargetMaskVaxIDs, [5,10,15,20,25])

    # 1 and 3 are random, 2 and 3 are watts on vax
    1 in DistributionTypes && append!(TargetNetworkIDs, [1,2,3,4,5,6,7,8,9,10])
    2 in DistributionTypes && append!(TargetNetworkIDs, [11,12,13,14,15,16,17,18,19,20])
    3 in DistributionTypes && append!(TargetNetworkIDs, [21,22,23,24,25,26,27,28,29,30])
    4 in DistributionTypes && append!(TargetNetworkIDs, [31,32,33,34,35,36,37,38,39,40])

    # select relevant data
    data = data[in(TargetNetworkIDs).(data.NetworkID), :]
    data = data[in(TargetMaskVaxIDs).(data.MaskVaxID), :]
    data = data[data.PeakDay .!= 0, :]

    histogram(data.PeakDay)
end

function plot_PeadDay_InfectedMax(data; VaxLevels = [], DistributionTypes = [])
    # Variable setting
    TargetMaskVaxIDs = []
    TargetNetworkIDs = []

    0 in VaxLevels && append!(TargetMaskVaxIDs, [1,6,11,16,21])
    20 in VaxLevels && append!(TargetMaskVaxIDs, [2,7,12,17,22])
    40 in VaxLevels && append!(TargetMaskVaxIDs, [3,8,13,18,23])
    60 in VaxLevels && append!(TargetMaskVaxIDs, [4,9,14,19,24])
    80 in VaxLevels && append!(TargetMaskVaxIDs, [5,10,15,20,25])

    # 1 and 3 are random, 2 and 3 are watts on vax
    1 in DistributionTypes && append!(TargetNetworkIDs, [1,2,3,4,5,6,7,8,9,10])
    2 in DistributionTypes && append!(TargetNetworkIDs, [11,12,13,14,15,16,17,18,19,20])
    3 in DistributionTypes && append!(TargetNetworkIDs, [21,22,23,24,25,26,27,28,29,30])
    4 in DistributionTypes && append!(TargetNetworkIDs, [31,32,33,34,35,36,37,38,39,40])

    # select relevant data
    data = data[in(TargetNetworkIDs).(data.NetworkID), :]
    data = data[in(TargetMaskVaxIDs).(data.MaskVaxID), :]
    @show data[2,:]
    plot(data, y=:AverageThickness, x=:InfectedMax, mode="markers")
end

function plot_thickness_epidemic(EpidemicIDs, connection)
    traces = GenericTrace[]
    for i in EpidemicIDs
        query = """
        SELECT 
            *,
            (CAST(H2Count AS DECIMAL) - CAST(H1Count AS DECIMAL))/(H0Count + H1Count + H2Count) AS Thickness 
        FROM PersistenceLoad
        WHERE EpidemicID = $i
        """
        data = run_query(query, connection) |> DataFrame
        
        trace =  scatter(
            data,
            x=:Distance,
            y=:Thickness,
            mode="lines"
            )
        push!(traces, trace)
    end
    layout = Layout(
        xaxis_title="Epsilon",
        yaxis_title="Thickness",
        title="Thickness over Epsilons: $EpidemicIDs"
        )
    display(plot(traces, layout))
end

function thickness_2(con)
    query = """
           WITH x AS (
               SELECT n
               FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
               ),
           y AS (
               SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID
               FROM x ones, x tens, x hundreds, x thousands, x tenthousands, x hundreadthousand
               )
           SELECT *
           FROM EpidemicSCMLoad_5
           JOIN y
           ON y.ID = EpidemicSCMLoad_5.EpidemicID
           WHERE EpidemicID >= 99998
           AND EpidemicID <= 100002
           ORDER BY EpidemicID
           """
    ResultDF = run_query(query, con) |> DataFrame

    SCMs = []
    for row in eachrow(ResultDF)
        SCM = epidemic_SCM_to_matrix(386, row[2])
        SCM = 1.0 ./ SCM
        append!(SCMs, [SCM])
    end

    DistanceMatrices = []
    for SCM in SCMs
        DistanceMatrix = floyd_warshall_shortest_paths(Graph(SCM), SCM).dists
        append!(DistanceMatrices, [DistanceMatrix])
    end

    PersistenceDiagrams = []
    for DistanceMatrix in DistanceMatrices
        append!(PersistenceDiagrams, [ripserer(DistanceMatrix; dim_max=2)])
    end

    RanksComputed = []
    for PersistenceDiagram in PersistenceDiagrams
        append!(RanksComputed, [compute_thickness_2(PersistenceDiagram)])
    end

    return RanksComputed
end

function compute_thickness_2(PersistenceDiagram)

    H0Epsilons = Tables.matrix(PersistenceDiagram[1])
    H1Epsilons = Tables.matrix(PersistenceDiagram[2])
    H2Epsilons = Tables.matrix(PersistenceDiagram[3])

    H1Count = length(PersistenceDiagram[2])
    H1CapIdx = (.90 * H1Count) |> floor |> Int
    H1CapEpsilon = sort(H1Epsilons[:,2],)[H1CapIdx]

    H2Count = length(PersistenceDiagram[3])
    H2CapIdx = (.90 * H2Count) |> floor |> Int
    H2CapEpsilon = sort(H2Epsilons[:,2],)[H2CapIdx]

    EpsilonCap = max(H2CapEpsilon, H1CapEpsilon)

    SignificantEpsilons::Vector{Float64} = vcat(H0Epsilons[:,1], H0Epsilons[:, 2], H1Epsilons[:,1], H1Epsilons[:, 2], H2Epsilons[:,1], H2Epsilons[:, 2])
    SignificantEpsilons = SignificantEpsilons |> unique |> sort

    Ranks = DataFrame(dist = SignificantEpsilons, h0 = zeros(length(SignificantEpsilons)), h1 = zeros(length(SignificantEpsilons)), h2 = zeros(length(SignificantEpsilons)), sum = zeros(length(SignificantEpsilons)))

    # Iterate over h0s
    for row in eachrow(H0Epsilons)
        Ranks[Ranks.dist .>= row[1] .&& Ranks.dist .< row[2], [2,5]] .+= 1.0
    end

    # Iterate over h1s
    for row in eachrow(H1Epsilons)
        Ranks[Ranks.dist .>= row[1] .&& Ranks.dist .< row[2], [3,5]] .+= 1.0
    end

    # Iterate over h2s
    for row in eachrow(H2Epsilons)
        Ranks[Ranks.dist .>= row[1] .&& Ranks.dist .< row[2], [4,5]] .+= 1.0
    end
    
    Ranks = Ranks[Ranks.dist .< EpsilonCap, :]
    RanksComputed = select(Ranks, :dist, :h0, :h1, :h2, :sum, [:h1, :h2, :sum] => ((h1, h2, sum) -> (h2 .- h1)./sum) => :tau)

    return RanksComputed
end

function closest_index(x, val)
    ibest = first(eachindex(x))
    dxbest = abs(x[ibest]-val)
    for I in eachindex(x)
        dx = abs(x[I]-val)
        if dx < dxbest
            dxbest = dx
            ibest = I
        end
    end
    ibest
end

function epsilon_stepped(data)
    epsilon = 10^(-5)
    distance = 0
    resultDF = DataFrame(EpidemicID = Int64[], Distance = Float64[], H0Count = Int64[], H1Count = Int64[], H2Count = Int64[], Sum = Float64[], Tau = Float64[])
    while distance < 0.115
        closest_idx = closest_index(data[:, 2], distance)
        append!(resultDF, DataFrame(data[closest_idx, :]))
        distance += epsilon
    end
    return resultDF
end

function interpolate_thickness(EpidemicID, con)
    temp = run_query("SELECT * FROM PersistenceLoad WHERE EpidemicID = $EpidemicID",con) |> DataFrame
    select!(temp, 
        :EpidemicID,
        :Distance, 
        :H0Count, 
        :H1Count, 
        :H2Count, 
        [:H1Count, :H2Count] => ((h1,h2) -> (Int.(h1) .+ Int.(h2))) => :Sum, 
        [:H1Count, :H2Count] => ((h1,h2) -> ((Int.(h2) .- Int.(h1)) ./ (Int.(h1) .+ Int.(h2)))) => :Tau 
    )

    temp = epsilon_stepped(temp)
    temp = temp[temp.Sum .>= 3, :]
    return temp
end

function mean_variance_interpolated(con)
    IDMeanVariance = []
    for EpidemicID in 100001:100500
        InterpolatedThicknessDF = interpolate_thickness(EpidemicID, con)
        Mean = mean(InterpolatedThicknessDF[:, 7])
        Variance = var(InterpolatedThicknessDF[:, 7])
        append!(IDMeanVariance, [EpidemicID, Mean, Variance])
    end

    return DataFrame(EpidemicID = IDMeanVariance[1:3:end], Mean = IDMeanVariance[2:3:end], Variance = IDMeanVariance[3:3:end])
end

function pure_recreation(EpidemicID, con)
    # Load the SCM
    query = """
        SELECT * 
        FROM EpidemicSCMLoad_5
        WHERE EpidemicID = $EpidemicID
    """
    SCMCompact = run_query(query, con) |> DataFrame
    SCM = epidemic_SCM_to_matrix(386, SCMCompact[1,2])

    CSV.write("RipVSGiotto.csv", Tables.table(SCM))

    # Transform SCM
    SCM = 1.0 ./ SCM
    SCM[diagind(SCM)] .= 0.0

    # Compute the Distance Matrix
    DistanceMatrix = floyd_warshall_shortest_paths(Graph(SCM), SCM).dists

    # Compute the Persistence Diagram
    PersistenceDiagram = ripserer(DistanceMatrix, dim_max = 2)

    # Count Features over evenly stepped time
    Epsilon = 10^(-5)
    SignficantEpsilons = birth.(PersistenceDiagram[1])
    append!(SignficantEpsilons, birth.(PersistenceDiagram[2]))
    append!(SignficantEpsilons, birth.(PersistenceDiagram[3]))
    unique!(SignficantEpsilons)
    deleteat!(SignficantEpsilons, SignficantEpsilons .== 0.0);
    Distance = minimum(SignficantEpsilons)
    
    H0Epsilons = DataFrame(Tables.matrix(PersistenceDiagram[1]), :auto)
    H1Epsilons = DataFrame(Tables.matrix(PersistenceDiagram[2]), :auto)
    H2Epsilons = DataFrame(Tables.matrix(PersistenceDiagram[3]), :auto)
    
    Ranks = DataFrame(Distance = Float64[], H0Count = Int64[], H1Count = Int64[], H2Count = Int64[])
    while Distance < 0.115
        H0Count = size(H0Epsilons[H0Epsilons.x1 .<= Distance .< H0Epsilons.x2, :], 1)
        H1Count = size(H1Epsilons[H1Epsilons.x1 .<= Distance .< H1Epsilons.x2, :], 1)
        H2Count = size(H2Epsilons[H2Epsilons.x1 .<= Distance .< H2Epsilons.x2, :], 1)

        append!(Ranks, DataFrame(Distance = Distance, H0Count = H0Count, H1Count = H1Count, H2Count = H2Count ))
        Distance += Epsilon
    end

    # Filter Rows 
    Ranks = Ranks[Ranks.H1Count .+ Ranks.H2Count .>= 3, :]

    # Compute Thickness
    select!(Ranks, :, [:H1Count, :H2Count] => ((h1,h2) -> ((h2 .- h1) ./ (h1 .+ h2))) => :Tau)
    Ranks[:, :RowCount] = 1:size(Ranks,1)

    return Ranks
    # Store Results
end

"""
Input a list of thickness computation dataframes with thickness in the 5th column
"""
function graphy(data)
    # Loop over each data element
    MVList = []
    i = 1
    for thickness in data
        # Compute the mean and variance
        Mean = mean(thickness[:, 5])
        Variance = var(thickness[:, 5])

        # Add Mean and Variance to List
        append!(MVList, [[i, Mean, Variance]])
        i += 1
    end

    return DataFrame(mapreduce(permutedims, vcat, MVList), :auto)
end


#==========================#
#    Paper Functions       #
#==========================#

"""
    epidemic_degree_distribution(EpidemicID)

Input an EpidemicID and return the degree distribution of the epidemic's SCM
"""
function epidemic_degree_distribution(EpidemicID, con)
    # Compute the TownID correlated to the EpidemicID
    query = """
        SELECT TownDim.TownID
        FROM EpidemicLoad
        JOIN EpidemicDim on EpidemicLoad.EpidemicID = EpidemicDim.EpidemicID
        JOIN BehaviorDim on EpidemicDim.BehaviorID = BehaviorDim.BehaviorID
        JOIN NetworkDim on BehaviorDim.NetworkID = NetworkDim.NetworkID
        JOIN TownDim on TownDim.TownID = NetworkDim.TownID
        WHERE EpidemicLoad.EpidemicID = $EpidemicID
    """
    TownID = run_query(query, con) |> DataFrame
    TownID = TownID[1,1] |> Int64
    
    # Load the SCM
    query = """
        SELECT * 
        FROM EpidemicSCMLoad_$TownID
        WHERE EpidemicID = $EpidemicID
    """
    SCMCompact = run_query(query, con) |> DataFrame
    SCM = SCMCompact[1,2] |> convert_to_vector
    
    # Count the number of occurences of each number in SCM
    DegreeDistribution = countmap(SCM)

    return DegreeDistribution
end

# Reverse the countmap function
function countmap_to_vector(countmap)
    DegreeDistribution = []
    for (key, value) in countmap
        append!(DegreeDistribution, [key for i in 1:value])
    end
    return DegreeDistribution
end

# Combine array of dict into one dict
function combine_dict(dict_array)
    combined_dict = Dict()
    for dict in dict_array
        for (key, value) in dict
            if key in keys(combined_dict)
                combined_dict[key] += value
            else
                combined_dict[key] = value
            end
        end
    end
    return combined_dict
end