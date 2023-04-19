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
       "GenmatchedJets.*", "GenmatchedHHBtagJets.*", genmatching_selector, "nGenJet", "Jet.*", 
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
    #     # a_1, b_1 = ak.unzip(
    #     #         ak.cartesian([a, b], axis=axis, nested=True)
    #     #         )
    #     mval = ak.Array(np.hypot(a.eta - b.eta, (a.phi - b.phi + np.pi) % (2 * np.pi) - np.pi))
    #     return mval


    # print("delta_r uses:", self.uses)

    # TODO: Also implement delta r values for partons and for matched genjets.
    padded_GenmatchedJets_eta = ak.pad_none(events.GenmatchedJets.eta, 2, axis=1)
    padded_GenmatchedHHBtagJets_eta = ak.pad_none(events.GenmatchedHHBtagJets.eta, 2, axis=1)
    padded_GenmatchedJets_phi = ak.pad_none(events.GenmatchedJets.phi, 2, axis=1)
    padded_GenmatchedHHBtagJets_phi = ak.pad_none(events.GenmatchedHHBtagJets.phi, 2, axis=1)

    padded_nan_GenmatchedJets_eta = ak.fill_none(padded_GenmatchedJets_eta, np.nan, axis=1)
    padded_nan_GenmatchedHHBtagJets_eta = ak.fill_none(padded_GenmatchedHHBtagJets_eta, np.nan, axis=1)
    padded_nan_GenmatchedJets_phi = ak.fill_none(padded_GenmatchedJets_phi, np.nan, axis=1)
    padded_nan_GenmatchedHHBtagJets_phi = ak.fill_none(padded_GenmatchedHHBtagJets_phi, np.nan, axis=1)

    def delta_r_jets(a_eta, a_phi, b_eta, b_phi):
        mval = ak.Array(np.hypot(a_eta - b_eta, (a_phi - b_phi + np.pi) % (2 * np.pi) - np.pi))
        return mval
    
    delta_r_2_matches = delta_r_jets(padded_nan_GenmatchedJets_eta[:,0],padded_nan_GenmatchedJets_phi[:,0],padded_nan_GenmatchedJets_eta[:,1],padded_nan_GenmatchedJets_phi[:,1])
    delta_r_2_matches = ak.where(np.isnan(delta_r_2_matches), EMPTY_FLOAT, delta_r_2_matches)
    delta_r_hhbtag = delta_r_jets(padded_nan_GenmatchedHHBtagJets_eta[:,0],padded_nan_GenmatchedHHBtagJets_phi[:,0],padded_nan_GenmatchedHHBtagJets_eta[:,1],padded_nan_GenmatchedHHBtagJets_phi[:,1])
    delta_r_hhbtag = ak.where(np.isnan(delta_r_hhbtag), EMPTY_FLOAT, delta_r_hhbtag)

    embed()
    # test_delta_r = delta_r_jets(padded_GenmatchedJets[:,0],padded_GenmatchedJets[:,1], axis=1)
    # embed()
    # mask_1 = ak.num(events.GenmatchedJets, axis=1) == 2
    # mask_2 = ak.num(events.GenmatchedHHBtagJets, axis=1) == 2

    # delta_r_2_matches = ak.where(mask_1, delta_r_jets(padded_nan_GenmatchedJets_eta[:,0], padded_nan_GenmatchedJets_phi[:,0], padded_nan_GenmatchedJets_eta[:,1], padded_nan_GenmatchedJets_phi[:,1]), EMPTY_FLOAT)
    # delta_r_hhbtag = ak.where(mask_2, delta_r_jets(padded_nan_GenmatchedHHBtagJets_eta[:,0],padded_nan_GenmatchedHHBtagJets_phi[:,0],padded_nan_GenmatchedHHBtagJets_eta[:,1],padded_nan_GenmatchedHHBtagJets_phi[:,1]), EMPTY_FLOAT)


    # def pad_events(column: ak.Array, number_required_objects: int):
    #         column_padded = ak.pad_none(column, number_required_objects, axis=1)
    #         column_padded = ak.fill_none(column_padded, EMPTY_FLOAT, axis=1)
    #         return column_padded
    
    # # calculate delta_r value between jet1 and jet2 for every event with at least 2 jets.
    # delta_r_2_matches = ak.where(mask_1, delta_r_jets(events.GenmatchedJets[mask_1][:,0],events.GenmatchedJets[mask_1][:,1], axis=1), EMPTY_FLOAT)
    # delta_r_hhbtag = ak.where(mask_2, delta_r_jets(events.GenmatchedHHBtagJets[mask_2][:,0],events.GenmatchedHHBtagJets[mask_2][:,1], axis=1), EMPTY_FLOAT)
    # embed()

    # delta_r_2_matches = ak.where(mask_1, events.GenmatchedJets[:,0].delta_r(events.GenmatchedJets[:,1]), EMPTY_FLOAT)
    # delta_r_2_matches_padded = pad_events(delta_r_2_matches, 1)

    # delta_r_2_matches_padded = pad_events(delta_r_2_matches, 1)
    # delta_r_hhbtag_padded = pad_events(delta_r_hhbtag, 1)

    events = set_ak_column_f32(events, "delta_r_2_matches", delta_r_2_matches[:,np.newaxis])
    events = set_ak_column_f32(events, "delta_r_HHbtag", delta_r_hhbtag[:,np.newaxis])
    return events

@producer(
    uses={
       "GenmatchedJets.*", "GenmatchedHHBtagJets.*", genmatching_selector, "nGenJet", "Jet.*", 
        attach_coffea_behavior
    },
    produces={
        # new columns
        "delta_r_2_matches", "delta_r_HHbtag",
    },
)
def genmatched_delta_r(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates new columns: 'delta_r_2_matches' for the delta r values of the first two genmatched jets
    and 'delta_r_HHbtag' for the delta r values of the first two HHBtag jets.
    """
    collections = {x: {"type_name" : "Jet"} for x in ["GenmatchedJets", "GenmatchedHHBtagJets"]}
    events = self[attach_coffea_behavior](events, collections=collections, **kwargs)

    all_deltars = events.GenmatchedJets.metric_table(events.GenmatchedJets)

    minimal_deltar_permutation = ak.firsts(all_deltars)

    real_deltars = minimal_deltar_permutation[minimal_deltar_permutation != 0]

    mask = ak.num(events.GenmatchedJets, axis=1) == 2

    delta_rs = ak.where(mask, ak.flatten(real_deltars), EMPTY_FLOAT)

    events = set_ak_column_f32(events, "delta_r_2_matches", delta_rs)
    embed()
    # events = set_ak_column_f32(events, "delta_r_HHbtag", delta_r_hhbtag[:,np.newaxis])
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
    first_pt_2_matches_padded = ak.pad_none(events.GenmatchedJets.pt, 1, axis=1)
    first_pt_btag_padded = ak.pad_none(events.GenmatchedHHBtagJets.pt, 1, axis=1)

    padded_nan_first_pt_2_matches = ak.fill_none(first_pt_2_matches_padded, np.nan, axis=1)
    padded_nan_first_pt_btag = ak.fill_none(first_pt_btag_padded, np.nan, axis=1)
    
    mask_1 = ak.count(events.GenmatchedJets.pt, axis=1) == 2
    mask_2 = ak.count(events.GenmatchedHHBtagJets.pt, axis=1) == 2

    first_pt_2_matches = ak.where(mask_1, padded_nan_first_pt_2_matches[:,0], EMPTY_FLOAT)
    first_pt_btag = ak.where(mask_2, padded_nan_first_pt_btag[:,0], EMPTY_FLOAT)

    # first_pt_2_matches_padded = pad_events(events.GenmatchedJets[mask_1][:,0].pt, 1)
    # first_pt_btag_padded = pad_events(events.GenmatchedHHBtagJets[mask_2][:,0].pt, 1)

    events = set_ak_column_f32(events, "first_pt_2_matches", first_pt_2_matches)
    events = set_ak_column_f32(events, "first_pt_btag", first_pt_btag)
    return events