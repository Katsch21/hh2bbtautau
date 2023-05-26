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
def invariant_mass(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates new column for the invariant mass of selected bjets.
    """
    # attach four momenta behavior for the field `Jet`
    # for more information see documentation of attach_coffea_behavior
    collections = {x: {"type_name" : "Jet"} for x in ["HHBJet"]}
    events = self[attach_coffea_behavior](events, collections=collections, **kwargs)

    def calculate_mass(array: ak.Array):
        # now add the four momenta of the first two `Jet`s using the
        # `sum` function of coffea's LorentzVector class
        dijets = array[:, :2].sum(axis=1)

        # the `add` operation is only meaningful if we have at leas two
        # jets, so create a selection accordingly
        dijet_mask = ak.num(array, axis=1) >= 2

        # now add the new column to the array *events*. The column contains
        # the mass of the summed four momenta where there are at least
        # two jets, and the default value `EMPTY_FLOAT` otherwise.
        col = ak.where(dijet_mask, dijets.mass, EMPTY_FLOAT)
        return col

    events.set_ak_column_f32(
            events,
            "HHBJet_dijet_mass",
            calculate_mass(events.HHBJet)
    )
    return events
