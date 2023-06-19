# coding: utf-8

"""
Definition of categories.
"""

import order as od

from columnflow.config_util import add_category


def add_categories(config: od.Config) -> None:
    """
    Adds all categories to a *config*.
    """
    add_category(
        config,
        name="incl",
        id=1,
        selection="sel_incl",
        label="inclusive",
    )
    add_category(
        config,
        name="2j",
        id=100,
        selection="sel_2j",
        label="2 jets",
    )
    add_category(
        config,
        name="ak4_sel",
        id=101,
        selection="sel_incl",
        label=r"$n_{jets, AK4} \geq 2$",
    )
    add_category(
        config,
        name="ak8_sel",
        id=102,
        selection="sel_incl",
        label=r"$n_{jets, AK4} < 2$",
    )
    add_category(
        config,
        name="ak8_sel_fatjet_tag",
        id=103,
        selection="sel_incl",
        label=r"$n_{jets, AK4} < 2$, AK8 jet tagging",
    )
    add_category(
        config,
        name="ak8_sel_subjet_tag",
        id=104,
        selection="sel_incl",
        label=r"$n_{jets, AK4} < 2$, Subjet tagging",
    )
    add_category(
        config,
        name="complete_phasespace",
        id=105,
        selection="sel_incl",
        label=r"Entire phase space",
    )