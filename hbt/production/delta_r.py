# coding: utf-8

"""
Calculate Delta R values to check GenJets and btags.
"""

import law
import functools

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column
from hbt.selection.genmatching import genmatching_selector

ak = maybe_import("awkward")
np = maybe_import("numpy")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)
set_ak_column_i32 = functools.partial(set_ak_column, value_type=np.int32)


@producer(
    uses={
        genmatching_selector.PRODUCES
    },
    produces={
        # new columns
        "delta_r_2_matches", "delta_r_btag",
    },
)
def delta_r(self: Producer, events: ak.Array) -> ak.Array:

    # check to assert that there are really two jets present
    mask_1 = ak.count(events.Jet.GenmatchedJets) == 2
    mask_2 = ak.count(events.Jet.GenmatchedHHBtagJets) == 2

    events = set_ak_column_f32(events, "delta_r_2_matches", events.Jet.GenmatchedJets[mask_1][:,0].delta_r(events.Jet.GenmatchedJets[mask_1][:,1]))
    events = set_ak_column_f32(events, "delta_r_HHbtag", events.Jet.GenmatchedHHBtagJets[mask_2][:,0].delta_r(events.Jet.GenmatchedHHBtagJets[mask_2][:,1]))
    return events



@producer(
    uses={
        genmatching_selector.PRODUCES
    },
    produces={
        # new columns
        "first_pt_2_matches", "first_pt_btag",
    },
)
def get_pt(self: Producer, events: ak.Array) -> ak.Array:

    # check to assert that there are really two jets present
    mask_1 = ak.count(events.Jet.GenmatchedJets) == 2
    mask_2 = ak.count(events.Jet.GenmatchedHHBtagJets) == 2

    events = set_ak_column_f32(events, "first_pt_2_matches", events.Jet.GenmatchedJets[mask_1][:,0].pt)
    events = set_ak_column_f32(events, "first_pt_btag", events.Jet.GenmatchedHHBtagJets[mask_2][:,0].pt)
    return events
