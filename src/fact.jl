# Fact methods used in loading FCT schema.

#=
Primary functions for running all FCT methods.
=#

function _load_fact_tables(connection)
    _create_fact_schema(connection)
    _load_epidemic_fact_table(connection)
end

function _create_fact_schema(connection)
    query = """
        CREATE SCHEMA IF NOT EXISTS FCT;
    """
    _run_query(query, connection = connection)
end

#=
Table specific methods
=#

function _load_epidemic_fact_table(connection)
    query = """
        DROP TABLE IF EXISTS FCT.EpidemicReport;
    """
    _run_query(query, connection = connection)

    query = """
        CREATE TABLE FCT.EpidemicReport(
            EpidemicID INTEGER,
            BehaviorID INTEGER, 
            NetworkID INTEGER, 
            TownID INTEGER, 
            MaskVaxID INTEGER, 
            MaskPortion DECIMAL, 
            VaxPortion DECIMAL, 
            MeanInfectedMaskAndVaxAgentProbability DECIMAL,
            PRIMARY KEY (EpidemicID, BehaviorID, NetworkID, TownID)
            );
    """
    _run_query(query, connection = connection)

    query = """
    INSERT INTO FCT.EpidemicReport
    SELECT
        EpidemicID,
        BehaviorID,
        NetworkID,
        TownID,
        MaskVaxID,
        MaskPortion,
        VaxPortion,
        AVG(InfectedMaskAndVaxAgentProbability) AS MeanInfectedMaskAndVaxAgentProbability
    FROM STG.EpidemicResults
    GROUP BY 
        EpidemicID,
        BehaviorID,
        NetworkID,
        TownID,
        MaskVaxID,
        MaskPortion,
        VaxPortion
    """
    _run_query(query, connection = connection)
    return true
    
end

