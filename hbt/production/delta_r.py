# coding: utf-8

"""
Calculate Delta R values pt of first jet and sum of 1st and 2nd jet pt.
"""

import law
import functools

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column
from columnflow.production.util import attach_coffea_behavior
from hbt.selection.genmatching import genmatching_selector
from hbt.production.gen_HH_decay import gen_HH_decay_products
from IPython import embed

ak = maybe_import("awkward")
np = maybe_import("numpy")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)
set_ak_column_i32 = functools.partial(set_ak_column, value_type=np.int32)

# old producer delta_r. Is not used anymore, but don't just delete those lines as the columns would be missing!
# now using genmatched_delta_r()
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
    Old delta R producer.
    Creates new columns: 'delta_r_2_matches' for the delta r values of the first two gegenmatched_delta_r
    """
    events = self[attach_coffea_behavior](events, **kwargs)

    
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
    Creates new columns for delta R values of Gen Matching steps:
        'delta_r_genbpartons' for the two generated partons
        'delta_r_genmatchedgenjets' for the first two Gen matched Gen jets
        'delta_r_2_matches' for the first two Detector jets
        'delta_r_HHbtag' for the first two matched and selected Detector jets.
    :param events: all events
    :return: events with new columns for delta R values
    """

    collections = {x: {"type_name" : "Jet"} for x in ["GenmatchedJets", "GenmatchedHHBtagJets"]}
    collections.update({y: {"type_name" : "GenParticle", "skip_fields": "*Idx*G",} for y in ["GenBPartons"]})
    collections.update({y: {"type_name" : "Jet", "skip_fields": "*Idx*G",} for y in ["GenmatchedGenJets"]})

    
    events = self[attach_coffea_behavior](events, collections=collections, **kwargs)

    def calculate_delta_r(array: ak.Array, num_objects: int=2):
        # calculate all possible delta R values (all permutations):
        all_deltars = array.metric_table(array)
        min_deltars_permutations = ak.firsts(all_deltars)
        real_deltar_mask = min_deltars_permutations != 0
        # real_deltars = ak.mask(min_deltars_permutations, real_deltar_mask)
        real_deltars = min_deltars_permutations[real_deltar_mask]
        real_deltars_padded = ak.pad_none(real_deltars, 1, axis=1)
        real_deltars_filled = ak.fill_none(real_deltars_padded, 0, axis=1)
        # still Nones in axis 0
        # new_mask = ak.is_none(real_deltars_filled)
        real_deltars_filled_axiszero = ak.fill_none(real_deltars_filled, [0], axis=0)
        mask = ak.num(array, axis=1) == num_objects
        # embed()
        return ak.where(mask, ak.flatten(real_deltars_filled_axiszero), EMPTY_FLOAT)
    # embed()
    events = set_ak_column_f32(events, "delta_r_genbpartons", calculate_delta_r(events.GenBPartons))
    events = set_ak_column_f32(events, "delta_r_genmatchedgenjets", calculate_delta_r(events.GenmatchedGenJets))
    events = set_ak_column_f32(events, "delta_r_2_matches", calculate_delta_r(events.GenmatchedJets))
    events = set_ak_column_f32(events, "delta_r_HHbtag", calculate_delta_r(events.GenmatchedHHBtagJets))
    # embed()
    return events

@producer(
    uses={
        genmatching_selector.PRODUCES, genmatching_selector,
    },
    produces={
        # new columns
        "first_pt_2_matches", "first_pt_btag", "first_pt_genbpartons", "first_pt_genmatchedgenjets",
        "sum_pt_2_matches", "sum_pt_btag", "sum_pt_genbpartons", "sum_pt_genmatchedgenjets"
    },
)
def get_pt(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """Creates new columns for first (sorted after hardest Gen Parton) parton/jet: 
        'first_pt_genbpartons' for the transverse momentum of the first Gen Parton,
        'first_pt_2_matches' for the pt of the first genmatched Gen jet and
        'first_pt_btag' for the transverse momentum of the first matched and HHbtag selected Detector jet.
    Creates new columns for sum (sum over pT, not over four-vectors) of first and second parton/jet: 
        'first_pt_genbpartons' for the transverse momentum of the first Gen Parton,
        'first_pt_2_matches' for the pt of the first genmatched Gen jet and
        'first_pt_btag' for the transverse momentum of the first matched and HHbtag selected Detector jet.

    :param events: all events
    :return: events with new first pT and sum pT columns
    """

    def first_pt(array: ak.Array, num_objects: int=2):
        first_pt_padded = ak.pad_none(array.pt, 1, axis=1)
        padded_nan_first_pt = ak.fill_none(first_pt_padded, np.nan, axis=1)
        mask = ak.count(array.pt, axis=1) == num_objects
        first_pt = ak.where(mask, padded_nan_first_pt[:,0], EMPTY_FLOAT)
        return first_pt
    
    def second_pt(array: ak.Array, num_objects: int=2):
        second_pt_padded = ak.pad_none(array.pt, 1, axis=1)
        padded_nan_second_pt = ak.fill_none(second_pt_padded, np.nan, axis=1)
        mask = ak.count(array.pt, axis=1) == num_objects
        # embed()
        # pad the events with only one entry:
        ultimate_padded_second_pt = ak.pad_none(padded_nan_second_pt, 2, axis=1)
        second_pt = ak.where(mask, ultimate_padded_second_pt[:,1], EMPTY_FLOAT) ##
        return second_pt

    # produce pt sums
    sum_pt_genbpartons = first_pt(events.GenBPartons) + second_pt(events.GenBPartons)
    sum_pt_genmatchedgenjets = first_pt(events.GenmatchedGenJets) + second_pt(events.GenmatchedGenJets)
    sum_pt_2_matches = first_pt(events.GenmatchedJets) + second_pt(events.GenmatchedJets) ##
    sum_pt_btag = first_pt(events.GenmatchedHHBtagJets) + second_pt(events.GenmatchedHHBtagJets)


    # variables for 1st (btag ordered) pt
    events = set_ak_column_f32(events, "first_pt_genbpartons", first_pt(events.GenBPartons))
    events = set_ak_column_f32(events, "first_pt_genmatchedgenjets", first_pt(events.GenmatchedGenJets))
    events = set_ak_column_f32(events, "first_pt_2_matches", first_pt(events.GenmatchedJets))
    events = set_ak_column_f32(events, "first_pt_btag", first_pt(events.GenmatchedHHBtagJets))

    # variables for sum of 1st+2nd (btag ordered) pt
    events = set_ak_column_f32(events, "sum_pt_genbpartons", sum_pt_genbpartons)
    events = set_ak_column_f32(events, "sum_pt_genmatchedgenjets", sum_pt_genmatchedgenjets)
    events = set_ak_column_f32(events, "sum_pt_2_matches", sum_pt_2_matches)
    events = set_ak_column_f32(events, "sum_pt_btag", sum_pt_btag)

    return events

@producer(
    uses={
        "GenBpartons.*", genmatching_selector, attach_coffea_behavior,
        # "GenPart.pt", "GenPart.eta", "GenPart.phi", "GenPart.pdgId",

    },
    produces={
        "delta_r_partons_boosted",
    },
)
def partons_delta_r(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """Creates new column: 'delta_r_partons_boosted' for delta R value of the two Gen Partons.

    :param events: all events
    :return: events with new column: Delta R of Gen partons
    """

    collections = {x: {"type_name" : "GenParticle", "skip_fields": "*Idx*G",} for x in ["GenBpartons"]}

    events = self[attach_coffea_behavior](events, collections=collections, **kwargs)
    
    def calculate_delta_r(array: ak.Array, num_objects: int=2):
        # calculate all possible delta R values (all permutations):
        all_deltars = array.metric_table(array)
        min_deltars_permutations = ak.firsts(all_deltars)
        real_deltar_mask = min_deltars_permutations != 0
        # real_deltars = ak.mask(min_deltars_permutations, real_deltar_mask)
        real_deltars = min_deltars_permutations[real_deltar_mask]
        real_deltars_padded = ak.pad_none(real_deltars, 1, axis=1)
        real_deltars_filled = ak.fill_none(real_deltars_padded, 0, axis=1)
        # still Nones in axis 0
        # new_mask = ak.is_none(real_deltars_filled)
        real_deltars_filled_axiszero = ak.fill_none(real_deltars_filled, [0], axis=0)
        mask = ak.num(array, axis=1) == num_objects
        # embed()
        # from IPython import embed
        # embed()
        return ak.where(mask, ak.flatten(real_deltars_filled_axiszero), EMPTY_FLOAT)
    
    #embed()
    events = set_ak_column_f32(events, "delta_r_partons_boosted", calculate_delta_r(events.GenBpartons)[...,np.newaxis])

    return events
