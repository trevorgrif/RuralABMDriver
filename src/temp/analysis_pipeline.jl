using DuckDB
using DataFrames
using StatsBase
using PlotlyJS

#==============#
# Watts Effect #
#==============#

"""
    watts_statistical_test(target_distribution = 0)

Perform a statistical t-test (Welch's T-Test) to determine if the average of InfectedMaskAndVaxAgentProbability is significantly different between town builds at the given target distribution.

Returns an array of test results between all four town builds.

# Note: target_distribution = 10 * mask_portion + vax_portion
"""
function watts_statistical_test(target_distribution = 0)
    data = create_con() |> get_epidemic_level_data
    data = data[data.TownID .< 5, :]

    data_1 = data[data.TownID .== 1, :]
    data_2 = data[data.TownID .== 2, :]
    data_3 = data[data.TownID .== 3, :]
    data_4 = data[data.TownID .== 4, :]

    select!(data_1, :TownID, [:MaskPortion, :VaxPortion] => ((m,v) -> 10*m+v), :InfectedMaskAndVaxAgentProbability)
    select!(data_2, :TownID, [:MaskPortion, :VaxPortion] => ((m,v) -> 10*m+v), :InfectedMaskAndVaxAgentProbability)
    select!(data_3, :TownID, [:MaskPortion, :VaxPortion] => ((m,v) -> 10*m+v), :InfectedMaskAndVaxAgentProbability)
    select!(data_4, :TownID, [:MaskPortion, :VaxPortion] => ((m,v) -> 10*m+v), :InfectedMaskAndVaxAgentProbability)

    ProtectedInfectionProbability_1 = convert.(Float64, data_1[data_1.MaskPortion_VaxPortion_function .== target_distribution, :InfectedMaskAndVaxAgentProbability])
    ProtectedInfectionProbability_2 = convert.(Float64, data_2[data_2.MaskPortion_VaxPortion_function .== target_distribution, :InfectedMaskAndVaxAgentProbability])
    ProtectedInfectionProbability_3 = convert.(Float64, data_3[data_3.MaskPortion_VaxPortion_function .== target_distribution, :InfectedMaskAndVaxAgentProbability])
    ProtectedInfectionProbability_4 = convert.(Float64, data_4[data_4.MaskPortion_VaxPortion_function .== target_distribution, :InfectedMaskAndVaxAgentProbability])

    Results = []
    append!(Results, [UnequalVarianceTTest(ProtectedInfectionProbability_1, ProtectedInfectionProbability_2)])
    append!(Results, [UnequalVarianceTTest(ProtectedInfectionProbability_1, ProtectedInfectionProbability_3)])
    append!(Results, [UnequalVarianceTTest(ProtectedInfectionProbability_1, ProtectedInfectionProbability_4)])
    append!(Results, [UnequalVarianceTTest(ProtectedInfectionProbability_2, ProtectedInfectionProbability_3)])
    append!(Results, [UnequalVarianceTTest(ProtectedInfectionProbability_2, ProtectedInfectionProbability_4)])
    append!(Results, [UnequalVarianceTTest(ProtectedInfectionProbability_3, ProtectedInfectionProbability_4)])

    return Results

end

"""
    plot_probability_infection_protected()

Plot the average of InfectedMaskAndVaxAgentProbability aggregated by MaskVaxID for each town build. Evidence of `Watts Effect`.
"""
function plot_probability_infection_protected()
    data = create_con() |> get_epidemic_level_data
    data = data[data.TownID .< 5, :]
    data = epidemic_level_computed_statistics(data)

    # Filter out 0% infection probability
    data = data[data.InfectedMaskAndVaxAgentProbability_mean .> 0, :]

    data_1 = data[data.TownID .== 1, :]
    data_2 = data[data.TownID .== 2, :]
    data_3 = data[data.TownID .== 3, :]
    data_4 = data[data.TownID .== 4, :]

    plot(
        [
            scatter(
            data_1, 
            name = "Town 1 (R,R)",
            x=:MaskPortion_VaxPortion_function, 
            y=:InfectedMaskAndVaxAgentProbability_mean, 
            facet_color=:TownID,
            mode="markers"
            ),
            scatter(
            data_2,
            name = "Town 2 (W,W)",
            x=:MaskPortion_VaxPortion_function,
            y=:InfectedMaskAndVaxAgentProbability_mean,
            facet_color=:TownID,
            mode="markers"
            ),
            scatter(
            data_3,
            name = "Town 3 (W,R)",
            x=:MaskPortion_VaxPortion_function,
            y=:InfectedMaskAndVaxAgentProbability_mean,
            facet_color=:TownID,
            mode="markers"
            ),
            scatter(
            data_4,
            name = "Town 4 (R,W)",
            x=:MaskPortion_VaxPortion_function,
            y=:InfectedMaskAndVaxAgentProbability_mean,
            facet_color=:TownID,
            mode="markers"
            )
        ],
        Layout(
            legend = attr(
                y = 1.02,
                x = 1,
                yanchor="bottom",
                xanchor="right",
                orientation="h"
            ),
            title="Probability of Infection for Protected Agents",
            xaxis_title="Masking and Vaccination Distribution (10*m+v)",
            yaxis_title="Probability of Infection for Protected Agents",
            legend_title="TownID",
            legend_orientation="h"
        )
    )
end