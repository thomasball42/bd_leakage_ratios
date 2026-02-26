# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 10:49:17 2024

@author: Thomas Ball

"""

from typing import Any
import matplotlib.pyplot as plt

A: float = 0.8
S: float = 0.5

# Colours for the different food groups in the Clark et al. 2022 PNAS paper
colours_stim: dict[str, str] = {
    "Ruminant meat": "#C90D75",
    "Pig meat": "#D64A98",
    "Poultry meat": "#D880B1",
    "Dairy": "#F7BDDD",
    "Eggs": "#FFEDF7",
    "Grains": "#D55E00",
    "Rice": "#D88E53",
    "Soybeans": "#DCBA9E",
    "Roots and tubers": "#0072B2",
    "Vegetables": "#4F98C1",
    "Legumes and pulses": "#9EBFD2",
    "Bananas": "#FFED00",
    "Tropical fruit": "#FFF357",
    "Temperate fruit": "#FDF8B9",
    "Tropical nuts": "#27E2FF",
    "Temperate nuts": "#7DEEFF",
    "Sugar beet": "#FFC000",
    "Sugar cane": "#F7C93B",
    "Spices": "#009E73",
    "Coffee": "#33CCA2",
    "Cocoa": "#62DEBC",
    "Tea and maté": "#A2F5DE",
    "Oilcrops": "#000000",
    "Other": "#A2A2A2",
}

def bar_chart(
    ax: Any, benefit: float, leakage: float, labels: list[str] | None = None
) -> None:
    """Creates a bar chart of the avoided extinctions of returning 1000km2 of cropland
    back to natural habitat

    :param ax: the current subplot that is being graphed
    :param benefit: the number of extinctions that would be avoided in the country investigated
    :param leakage: the number of extinctions that would occur elsewhere to meet demand
    :param labels: the labels of the two bars
    """
    ax.bar(0, benefit, color="b", alpha=A, label=(labels[0] if labels else None))
    ax.bar(1, -leakage, color="orange", alpha=A, label=(labels[1] if labels else None))
    xlim = ax.get_xlim()
    ax.hlines(0, *xlim, color="k", linewidth=1)
    ax.set_xlim(xlim)
    ax.set_xticks([])
    ax.set_ylabel("Avoided extinctions ($\\Delta$E / km$^2$)")


def box_plot(ax: Any, upper_plot: Any, lower_plot: Any) -> None:
    """Calculating a box plot of the mean yield or th change in extinctions per area

    :param ax: the current subplot that is being graphed
    :param upper_plot: a list expressing the yield (kg/km2) or biodiversity (/km2)
    :param lower_plot: a series expressing the cost of the yield or biodiversity"""
    ax.boxplot(upper_plot, positions=[S], whis=None, conf_intervals=None, vert=False)
    ax.boxplot(lower_plot, positions=[0], vert=False, showfliers=False, whis=(0, 100))
    ax.set_yticks([])
    # ax.set_xlim(0, ax.get_xlim()[1])
    ax.hlines(S / 2, *ax.get_xlim(), color="k", linewidth=1)

def leakage_yield_bd_boxes(item_name,
                           internal_bd_benefit, external_bd_leakage, 
                           internal_yield_kg_km2, external_yield_kg_km2,
                           internal_bd_km2, external_bd_km2):
    
    # PREP FOR PLOT_SCRIPT
    labels = ["Domestic gain", "Market leakage loss"]
    heading2: str = "Mean yield kg/km$^2$"
    heading3: str = "10$^{-5}$$\\Delta$E/km$^2$"
    # PLOT SCRIPT
    fig, saxs = plt.subplots(1, 3)  
    bar_chart(saxs[0], internal_bd_benefit, external_bd_leakage, labels) 
    ax = saxs[1]
    ax.set_xlabel(heading2)
    box_plot(ax, internal_yield_kg_km2, external_yield_kg_km2)
    ax = saxs[2]
    ax.set_xlabel(heading3)
    box_plot(ax, internal_bd_km2, external_bd_km2)
    saxs[1].set_title(item_name[0])
    fig.tight_layout()
    plt.show()