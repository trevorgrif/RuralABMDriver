
"""
SELECT 
    B.EpidemicID,
    list_value(B.Value, B.Count) AS DegreeDistribution
FROM (
    SELECT 
        EpidemicID, 
        A.Value AS Value,
        COUNT(A.Value) AS Count 
    FROM (
        SELECT 
            EpidemicSCMLoad_1.EpidemicID, 
            UNNEST(str_split(SCM, ',')) AS Value 
        FROM EpidemicSCMLoad_1 
        WHERE EpidemicID in (1,2)
        ) AS A 
    GROUP BY 
        EpidemicID, 
        A.Value
    ) AS B
GROUP BY
    B.EpidemicID
"""
