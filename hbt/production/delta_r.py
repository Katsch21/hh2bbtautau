# coding: utf-8

"""
Calculate Delta R values and pt of first jet to check GenJets and btags.
"""

import law
import functools

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column
from columnflow.production.util import attach_coffea_behavior
from hbt.selection.genmatching import genmatching_selector
from IPython import embed

ak = maybe_import("awkward")
np = maybe_import("numpy")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)
set_ak_column_i32 = functools.partial(set_ak_column, value_type=np.int32)


@producer(
    uses={
       "GenmatchedJets", "GenmatchedHHBtagJets", genmatching_selector, "nGenJet", "Jet.*", 
        attach_coffea_behavior
    },
    produces={
        # new columns
        "delta_r_2_matches", "delta_r_HHbtag",
    },
)
def delta_r(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates new columns: 'delta_r_2_matches' for the delta r values of the first two genmatched jets
    and 'delta_r_HHbtag' for the delta r values of the first two HHBtag jets.
    """
    events = self[attach_coffea_behavior](events, **kwargs)

    # function to calculate delta r value of a jet pair
    # not necessary any more, as metric_table works again
    # def delta_r_jets(a, b, axis):
    #     a_1, b_1 = ak.unzip(
    #             ak.cartesian([a, b], axis=axis, nested=True)
    #             )
    #     mval = np.hypot(a_1.eta - b_1.eta, (a_1.phi - b_1.phi + np.pi) % (2 * np.pi) - np.pi)
    #     return mval

    # print("delta_r uses:", self.uses)

    mask_1 = ak.count(events.GenmatchedJets.pt, axis=1) == 2
    mask_2 = ak.count(events.GenmatchedHHBtagJets.pt, axis=1) == 2


    def pad_events(column: ak.Array, number_required_objects: int):
            column_padded = ak.pad_none(column, number_required_objects, axis=1)
            column_padded = ak.fill_none(column_padded, EMPTY_FLOAT, axis=1)
            return column_padded
    
    embed()
    # calculate delta_r value between jet1 and jet2 for every event with at least 2 jets.
    delta_r_2_matches = ak.where(mask_1, events.GenmatchedJets[mask_1][:,0].metric_table(events.GenmatchedJets[mask_1][:,1], axis=1))
    delta_r_hhbtag = ak.where(mask_2, events.GenmatchedHHBtagJets[mask_2][:,0].metric_table(events.GenmatchedHHBtagJets[mask_2][:,1], axis=1))

    # delta_r_2_matches = ak.where(mask_1, events.GenmatchedJets[:,0].delta_r(events.GenmatchedJets[:,1]), EMPTY_FLOAT)
    # delta_r_2_matches_padded = pad_events(delta_r_2_matches, 1)

    delta_r_2_matches_padded = pad_events(delta_r_2_matches, 1)
    delta_r_hhbtag_padded = pad_events(delta_r_hhbtag, 1)

    events = set_ak_column_f32(events, "delta_r_2_matches", delta_r_2_matches_padded)
    events = set_ak_column_f32(events, "delta_r_HHbtag", delta_r_hhbtag_padded)
    return events



@producer(
    uses={
        genmatching_selector.PRODUCES, genmatching_selector,
    },
    produces={
        # new columns
        "first_pt_2_matches", "first_pt_btag",
    },
)
def get_pt(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates new columns: 'first_pt_2_matches' for the transverse momentum of the first genmatched jet
    and 'first_pt_btag' for the transverse momentum of the first HHBtag jet.
    """
    # check to assert that therlaw run cf.PlotVariables2D --version v1 --variables jet1_pt-n_jet --processes graviton_hh_ggf_bbtautau_m1250 --categories incl --config run2_2017_nano_uhh_v11_limited --skip-legende are really two jets present
    mask_1 = ak.count(events.GenmatchedJets.pt, axis=1) == 2
    mask_2 = ak.count(events.GenmatchedHHBtagJets.pt, axis=1) == 2

    def pad_events(column: ak.Array, number_required_objects: int):
        column_padded = ak.pad_none(column, number_required_objects, axis=1)
        column_padded = ak.fill_none(column_padded, EMPTY_FLOAT, axis=1)
        return column_padded

    first_pt_2_matches_padded = pad_events(events.GenmatchedJets[mask_1][:,0].pt, 1)
    first_pt_btag_padded = pad_events(events.GenmatchedHHBtagJets[mask_2][:,0].pt, 1)

    events = set_ak_column_f32(events, "first_pt_2_matches", first_pt_2_matches_padded)
    events = set_ak_column_f32(events, "first_pt_btag", first_pt_btag_padded)
    return events