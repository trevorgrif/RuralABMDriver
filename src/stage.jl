# Run analysis on landing tables 

function _load_staging_tables(connection)
    _create_staging_schema(connection)
    _load_epidemic_results_table(connection)
end

function _create_staging_schema(connection)
    query = """
        CREATE SCHEMA IF NOT EXISTS STG;
    """
    run_query(query, connection = connection)
end

function _load_epidemic_results_table(connection)
    query = """
        DROP TABLE IF EXISTS STG.EpidemicResults;
    """
    run_query(query, connection = connection)

    query = """
        CREATE TABLE STG.EpidemicResults(
            EpidemicID INTEGER,
            BehaviorID INTEGER, 
            NetworkID INTEGER, 
            TownID INTEGER, 
            MaskVaxID INTEGER, 
            MaskPortion DECIMAL, 
            VaxPortion DECIMAL, 
            InfectedTotal INTEGER,
            InfectedMax INTEGER, 
            PeakDay INTEGER, 
            RecoveredTotal INTEGER, 
            RecoveredMasked INTEGER, 
            RecoveredVaccinated INTEGER, 
            RecoveredMaskAndVax INTEGER, 
            MaskedAgentTotal INTEGER, 
            VaxedAgentTotal INTEGER, 
            MaskAndVaxAgentTotal INTEGER, 
            InfectedMaskedAgentTotal INTEGER, 
            InfectedVaxedAgentTotal INTEGER, 
            InfectedMaskAndVaxAgentTotal INTEGER, 
            InfectedMaskAndVaxAgentProbability DECIMAL,
            DegreeDistribution MAP(INTEGER, INTEGER),
            PRIMARY KEY (EpidemicID, BehaviorID, NetworkID, TownID)
            );
    """
    run_query(query, connection = connection)

    query = """
    INSERT INTO STG.EpidemicResults
    SELECT DISTINCT
        EpidemicDim.EpidemicID,
        BehaviorDim.BehaviorID,
        NetworkDim.NetworkID,
        TownDim.TownID,
        BehaviorDim.MaskVaxID,
        MaskVaxDim.MaskPortion,
        MaskVaxDim.VaxPortion,
        EpidemicDim.InfectedTotal,
        EpidemicDim.InfectedMax,
        EpidemicDim.PeakDay,
        EpidemicDim.RecoveredTotal,
        EpidemicDim.RecoveredMasked,
        EpidemicDim.RecoveredVaccinated,
        EpidemicDim.RecoveredMaskAndVax,
        ProtectedTotals.MaskedAgentTotal,
        ProtectedTotals.VaxedAgentTotal,
        ProtectedTotals.MaskAndVaxAgentTotal,
        InfectedTotals.InfectedMaskedAgentTotal,
        InfectedTotals.InfectedVaxedAgentTotal,
        InfectedTotals.InfectedMaskAndVaxAgentTotal,
        CASE 
            WHEN ProtectedTotals.MaskAndVaxAgentTotal = 0 
            THEN 0 
            ELSE CAST(InfectedTotals.InfectedMaskAndVaxAgentTotal AS DECIMAL) / CAST(ProtectedTotals.MaskAndVaxAgentTotal AS DECIMAL) 
            END AS InfectedMaskAndVaxAgentProbability,
        list_histogram(split(EpidemicSCMLoad.SCM, ',')::int64[])
    FROM EpidemicDim
    LEFT JOIN BehaviorDim ON BehaviorDim.BehaviorID = EpidemicDim.BehaviorID
    LEFT JOIN NetworkDim ON NetworkDim.NetworkID = BehaviorDim.NetworkID
    LEFT JOIN TownDim ON TownDim.TownID = NetworkDim.TownID
    LEFT JOIN MaskVaxDim ON MaskVaxDim.MaskVaxID = BehaviorDim.MaskVaxID
    LEFT JOIN (
        -- Get the total number of infected agents in each protected class for each epidemic
        SELECT
            EpidemicDim.BehaviorID,
            EpidemicDim.EpidemicID,
            SUM(CASE WHEN TransmissionLoad.AgentID in (
                SELECT AgentLoad.AgentID
                FROM AgentLoad
                WHERE AgentLoad.IsMasking = 1
                AND AgentLoad.BehaviorID = EpidemicDim.BehaviorID
            ) THEN 1 ELSE 0 END) AS InfectedMaskedAgentTotal,
            SUM(CASE WHEN TransmissionLoad.AgentID in (
                SELECT AgentLoad.AgentID
                FROM AgentLoad
                WHERE AgentLoad.IsVaxed = 1
                AND AgentLoad.BehaviorID = EpidemicDim.BehaviorID
            ) THEN 1 ELSE 0 END) AS InfectedVaxedAgentTotal,
            SUM(CASE WHEN TransmissionLoad.AgentID in (
                SELECT AgentLoad.AgentID
                FROM AgentLoad
                WHERE AgentLoad.IsMasking = 1 AND AgentLoad.IsVaxed = 1
                AND AgentLoad.BehaviorID = EpidemicDim.BehaviorID
            ) THEN 1 ELSE 0 END) AS InfectedMaskAndVaxAgentTotal
        FROM EpidemicDim
        LEFT JOIN TransmissionLoad
        ON TransmissionLoad.EpidemicID = EpidemicDim.EpidemicID
        GROUP BY EpidemicDim.BehaviorID, EpidemicDim.EpidemicID
    ) InfectedTotals
    ON InfectedTotals.EpidemicID = EpidemicDim.EpidemicID
    LEFT JOIN (
        -- Get the total number of agents in each protected class
        SELECT DISTINCT
            AgentLoad.BehaviorID,
            SUM(CASE WHEN AgentLoad.IsMasking = 1 THEN 1 ELSE 0 END) AS MaskedAgentTotal,
            SUM(CASE WHEN AgentLoad.IsVaxed = 1 THEN 1 ELSE 0 END) AS VaxedAgentTotal,
            SUM(CASE WHEN AgentLoad.IsVaxed = 1 THEN AgentLoad.IsMasking ELSE 0 END) AS MaskAndVaxAgentTotal
        FROM AgentLoad
        GROUP BY AgentLoad.BehaviorID
    ) ProtectedTotals
    ON ProtectedTotals.BehaviorID = InfectedTotals.BehaviorID
    LEFT JOIN EpidemicSCMLoad 
    ON EpidemicSCMLoad.EpidemicID = EpidemicDim.EpidemicID
    """
    run_query(query, connection = connection) 
    
    return true
end