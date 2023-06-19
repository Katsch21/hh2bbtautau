# coding: utf-8

"""
Calculates the invariant mass of selected b jets.
"""

import law
import functools

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column
from columnflow.production.util import attach_coffea_behavior
from IPython import embed

ak = maybe_import("awkward")
np = maybe_import("numpy")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)
set_ak_column_i32 = functools.partial(set_ak_column, value_type=np.int32)

def calculate_mass(array: ak.Array, n_objects: int=2):
    """Calculates invariant mass of objects.

    :param array: Objects for which the invariant mass should be calculated.
    :param n_objects: Number of objects.
    :return: Invariant mass.
    """
    # now add the four momenta of the first two `Jet`s using the
    # `sum` function of coffea's LorentzVector class
    if n_objects > 1:
        dijets = array[:, :n_objects].sum(axis=1)
        # the `add` operation is only meaningful if we have at leas two
        # jets, so create a selection accordingly
        dijet_mask = ak.num(array, axis=1) >= n_objects
    elif n_objects == 1:
        dijets = array
        dijet_mask = ak.full_like(array, True)

    # now add the new column to the array *events*. The column contains
    # the mass of the summed four momenta where there are at least
    # two jets, and the default value `EMPTY_FLOAT` otherwise.
    col = ak.where(dijet_mask, dijets.mass, EMPTY_FLOAT)
    return col

@producer(
    uses={
       "GenmatchedHHBtagJets.*", "Jet.*", "HHBJet.*",
        attach_coffea_behavior
    },
    produces={
        # new columns
        "HHBJet_dijet_mass",
    },
)
def invariant_mass_hhbjet(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """Creates new column for the invariant mass of selected bjets.

    :param events: All events
    :return: events with new column for invariant mass of jets.
    """
    # attach four momenta behavior for the field `Jet`
    # for more information see documentation of attach_coffea_behavior
    collections = {x: {"type_name" : "Jet"} for x in ["HHBJet"]}
    events = self[attach_coffea_behavior](events, collections=collections, **kwargs)

    

    events.set_ak_column_f32(
            events,
            "HHBJet_dijet_mass",
            calculate_mass(events.HHBJet)
    )
    return events


@producer(
    uses={
       "FatJet.*", "HardestFatJet.*", "FirstFatJet.*",
        attach_coffea_behavior
    },
    produces={
        # new columns
        "HardestFatJet_disubjets_mass", "FirstFatJet_disubjets_mass",
        "HardestFatJet.mass", "FirstFatJet.mass",
    },
)
def invariant_mass_fatjets(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """Creates new column for the invariant mass of selected bjets.

    :param events: all events
    :return: Invariant mass of hardest fatjet
    """
    # attach four momenta behavior for the field `Jet`
    # for more information see documentation of attach_coffea_behavior
    collections = {x: {"type_name" : "FatJet"} for x in ["HardestFatJet", "FirstFatJet"]}
    events = self[attach_coffea_behavior](events, collections=collections, **kwargs)

    set_ak_column_f32(
            events,
            "HardestFatJet.mass",
            calculate_mass(events.HardestFatJet, n_objects=1)
    )
    # set_ak_column_f32(
    #         events,
    #         "FirstFatJet.mass",
    #         calculate_mass(events.FirstFatJet, n_objects=1)
    # )

    return events