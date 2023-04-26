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

# old producer delta_r
# now using genmatched_delta_r
# TODO: check if delta_r is still used / if set_ak_columns are still saved
@producer(
    uses={
       "GenmatchedJets.*", "GenmatchedHHBtagJets.*", "GenJet.*", genmatching_selector, "nGenJet", "Jet.*", 
        attach_coffea_behavior
    },
    produces={
        # new columns
        "delta_r_2_matches", "delta_r_HHbtag",
        # "delta_r_partons", "delta_r_genmatchedgenjets"
    },
)
def delta_r(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates new columns: 'delta_r_2_matches' for the delta r values of the first two gegenmatched_delta_r
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

    # padded_GenmatchedJets_eta = ak.pad_none(events.GenmatchedJets.eta, 2, axis=1)
    # padded_GenmatchedHHBtagJets_eta = ak.pad_none(events.GenmatchedHHBtagJets.eta, 2, axis=1)
    # padded_GenmatchedJets_phi = ak.pad_none(events.GenmatchedJets.phi, 2, axis=1)
    # padded_GenmatchedHHBtagJets_phi = ak.pad_none(events.GenmatchedHHBtagJets.phi, 2, axis=1)

    # padded_nan_GenmatchedJets_eta = ak.fill_none(padded_GenmatchedJets_eta, np.nan, axis=1)
    # padded_nan_GenmatchedHHBtagJets_eta = ak.fill_none(padded_GenmatchedHHBtagJets_eta, np.nan, axis=1)
    # padded_nan_GenmatchedJets_phi = ak.fill_none(padded_GenmatchedJets_phi, np.nan, axis=1)
    # padded_nan_GenmatchedHHBtagJets_phi = ak.fill_none(padded_GenmatchedHHBtagJets_phi, np.nan, axis=1)

    # def delta_r_jets(a_eta, a_phi, b_eta, b_phi):
    #     mval = ak.Array(np.hypot(a_eta - b_eta, (a_phi - b_phi + np.pi) % (2 * np.pi) - np.pi))
    #     return mval
    
    # delta_r_2_matches = delta_r_jets(padded_nan_GenmatchedJets_eta[:,0],padded_nan_GenmatchedJets_phi[:,0],padded_nan_GenmatchedJets_eta[:,1],padded_nan_GenmatchedJets_phi[:,1])
    # delta_r_2_matches = ak.where(np.isnan(delta_r_2_matches), EMPTY_FLOAT, delta_r_2_matches)
    # delta_r_hhbtag = delta_r_jets(padded_nan_GenmatchedHHBtagJets_eta[:,0],padded_nan_GenmatchedHHBtagJets_phi[:,0],padded_nan_GenmatchedHHBtagJets_eta[:,1],padded_nan_GenmatchedHHBtagJets_phi[:,1])
    # delta_r_hhbtag = ak.where(np.isnan(delta_r_hhbtag), EMPTY_FLOAT, delta_r_hhbtag)


    # old version of calculating delta R values

    # embed()delta_r_2_matches
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
       "GenmatchedJets.*", "GenmatchedHHBtagJets.*", "GenBPartons.*", "GenmatchedGenJets.*", genmatching_selector, "nGenJet", "Jet.*",
        attach_coffea_behavior
    },
    produces={
        # new columns
        "delta_r_2_matches", "delta_r_HHbtag", "delta_r_genbpartons", "delta_r_genmatchedgenjets"
    },
)
def genmatched_delta_r(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates new columns: 'delta_r_2_matches' for the delta r values of the first two genmatched jets
    and 'delta_r_HHbtag' for the delta r values of the first two HHBtag jets.
    """
    # TODO: Also implement delta r values for partons and for matched genjets.
    collections = {x: {"type_name" : "Jet"} for x in ["GenmatchedJets", "GenmatchedHHBtagJets"]}
    collections.update({y: {"type_name" : "GenParticle", "skip_fields": "*Idx*G",} for y in ["GenBPartons"]})
    collections.update({y: {"type_name" : "GenJet", "skip_fields": "*Idx*G",} for y in ["GenmatchedGenJets"]})

    
    events = self[attach_coffea_behavior](events, collections=collections, **kwargs)

    # calculate all possible delta R values (all permutations):
    def calculate_delta_r(array: ak.Array, num_objects: int=2):
        all_deltars = array.metric_table(array)
        min_deltars_permutations = ak.firsts(all_deltars)
        real_deltars = min_deltars_permutations[min_deltars_permutations != 0]
        mask = ak.num(array, axis=1) == num_objects
        return ak.where(mask, ak.flatten(real_deltars), EMPTY_FLOAT)

    events = set_ak_column_f32(events, "delta_r_genbpartons", calculate_delta_r(events.GenBPartons))
    events = set_ak_column_f32(events, "delta_r_genmatchedgenjets", calculate_delta_r(events.GenmatchedGenJets))
    events = set_ak_column_f32(events, "delta_r_2_matches", calculate_delta_r(events.GenmatchedJets))
    events = set_ak_column_f32(events, "delta_r_HHbtag", calculate_delta_r(events.GenmatchedHHBtagJets))

    return events

@producer(
    uses={
        genmatching_selector.PRODUCES, genmatching_selector,
    },
    produces={
        # new columns
        "first_pt_2_matches", "first_pt_btag", "first_pt_genbpartons", "first_pt_genmatchedgenjets"
    },
)
def get_pt(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates new columns: 'first_pt_genbpartons' for the transverse momentum of the first Gen parton,
    'first_pt_2_matches' for the pt of the first genmatched jet
    and 'first_pt_btag' for the transverse momentum of the first HHBtag jet.
    """

    def first_pt(array: ak.Array, num_objects: int=2):
        first_pt_padded = ak.pad_none(array.pt, 1, axis=1)
        padded_nan_first_pt = ak.fill_none(first_pt_padded, np.nan, axis=1)
        mask = ak.count(array.pt, axis=1) == num_objects
        first_pt = ak.where(mask, padded_nan_first_pt[:,0], EMPTY_FLOAT)
        return first_pt

    # # check to assert that there are really two jets present
    # first_pt_genbpartons_padded = ak.pad_none(events.GenBPartons.pt, 1, axis=1)
    # first_pt_2_matches_padded = ak.pad_none(events.GenmatchedJets.pt, 1, axis=1)
    # first_pt_btag_padded = ak.pad_none(events.GenmatchedHHBtagJets.pt, 1, axis=1)

    # padded_nan_first_pt_genbpartons = ak.fill_none(first_pt_genbpartons_padded, np.nan, axis=1)
    # padded_nan_first_pt_2_matches = ak.fill_none(first_pt_2_matches_padded, np.nan, axis=1)
    # padded_nan_first_pt_btag = ak.fill_none(first_pt_btag_padded, np.nan, axis=1)
    
    # mask_0 = ak.count(events.GenBPartons.pt, axis=1) == 2
    # mask_1 = ak.count(events.GenmatchedJets.pt, axis=1) == 2
    # mask_2 = ak.count(events.GenmatchedHHBtagJets.pt, axis=1) == 2

    # first_pt_genbpartons = ak.where(mask_0, padded_nan_first_pt_genbpartons[:,0], EMPTY_FLOAT)
    # first_pt_2_matches = ak.where(mask_1, padded_nan_first_pt_2_matches[:,0], EMPTY_FLOAT)
    # first_pt_btag = ak.where(mask_2, padded_nan_first_pt_btag[:,0], EMPTY_FLOAT)

    # first_pt_2_matches_padded = pad_events(events.GenmatchedJets[mask_1][:,0].pt, 1)
    # first_pt_btag_padded = pad_events(events.GenmatchedHHBtagJets[mask_2][:,0].pt, 1)

    events = set_ak_column_f32(events, "first_pt_genbpartons", first_pt(events.GenBPartons))
    events = set_ak_column_f32(events, "first_pt_genmatchedgenjets", first_pt(events.GenmatchedGenJets))
    events = set_ak_column_f32(events, "first_pt_2_matches", first_pt(events.GenmatchedJets))
    events = set_ak_column_f32(events, "first_pt_btag", first_pt(events.GenmatchedHHBtagJets))
    return events