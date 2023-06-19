# coding: utf-8

"""
Column production methods related to higher-level features.
"""

import functools

from columnflow.production import Producer, producer
from columnflow.production.categories import category_ids
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column
from hbt.production.fatjet_tagger import fatjet_tagging_variables
from IPython import embed


np = maybe_import("numpy")
ak = maybe_import("awkward")

# helpers
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)
set_ak_column_i32 = functools.partial(set_ak_column, value_type=np.int32)


@producer(
    uses={
        # nano columns
        "Electron.pt", "Muon.pt", "Jet.pt", "HHBJet.pt", "BJet.pt",
    },
    produces={
        # new columns
        "ht", "n_jet", "n_hhbtag", "n_electron", "n_muon",
    },
)
def features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    events = set_ak_column_f32(events, "ht", ak.sum(events.Jet.pt, axis=1))
    events = set_ak_column_i32(events, "n_jet", ak.num(events.Jet.pt, axis=1))
    # events = set_ak_column_i32(events, "n_hhbtag", ak.num(events.HHBJet.pt, axis=1))
    events = set_ak_column_i32(events, "n_electron", ak.num(events.Electron.pt, axis=1))
    events = set_ak_column_i32(events, "n_muon", ak.num(events.Muon.pt, axis=1))

    return events


@producer(
    uses={
        mc_weight, category_ids,fatjet_tagging_variables,
        # nano columns
        "Jet.pt", "Jet.eta", "Jet.phi", 
        # btags:
        "Jet.btagDeepFlavB", "Jet.btagDeepFlavCvB",
        # deeptau:
        "Tau.rawDeepTau2017v2p1VSe", "Tau.rawDeepTau2017v2p1VSjet", "Tau.rawDeepTau2017v2p1VSmu",
        # fatjets:
        "FatJet.btagHbb", "FatJet.btagDeepB",
    },
    produces={
        mc_weight, category_ids,
        # new columns
        "cutflow.n_jet", "cutflow.n_jet_selected", "cutflow.ht", "cutflow.jet1_pt",
        "cutflow.jet1_eta", "cutflow.jet1_phi", "cutflow.jet2_pt",
        # btags
        "cutflow.btagDeepFlavB1", "cutflow.btagDeepFlavCvB1", "cutflow.btagDeepFlavB2", "cutflow.btagDeepFlavCvB2",
        # deeptau
        "cutflow.rawDeepTau2017v2p1VSe1", "cutflow.rawDeepTau2017v2p1VSjet1", "cutflow.rawDeepTau2017v2p1VSmu1",
        "cutflow.rawDeepTau2017v2p1VSe2", "cutflow.rawDeepTau2017v2p1VSjet2", "cutflow.rawDeepTau2017v2p1VSmu2",
        # fatjets
        "cutflow.fatJet1.btagHbb", "cutflow.fatJet1.btagDeepb", "cutflow.fatJet2.btagHbb", "cutflow.fatJet2.btagDeepb", 
        # fatjet tagger output
        "cutflow.HardestFatJet.particleNet_HbbvsQCD", "cutflow.HardestFatJet.particleNetMD_Xbb", "cutflow.HardestFatJet.pt",
    },
)
def cutflow_features(
    self: Producer,
    events: ak.Array,
    object_masks: dict[str, dict[str, ak.Array]],
    **kwargs,
) -> ak.Array:
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    events = self[category_ids](events, **kwargs)

    # apply per-object selections
    selected_jet = events.Jet[object_masks["Jet"]["Jet"]]

    # add EMPTY_FLOAT default value for columns with empty collections
    def pad_events(events: ak.Array, variable: str, number_required_objects: int):
        column = Route(variable).apply(events)
        column_padded = ak.pad_none(column, number_required_objects, axis=1)
        column_padded = ak.fill_none(column_padded, EMPTY_FLOAT, axis=1)
        return column_padded

    # add feature columns
    events = set_ak_column_i32(events, "cutflow.n_jet", ak.num(events.Jet, axis=1))
    events = set_ak_column_i32(events, "cutflow.n_jet_selected", ak.num(selected_jet, axis=1))
    events = set_ak_column_f32(events, "cutflow.ht", ak.sum(selected_jet.pt, axis=1))
    events = set_ak_column_f32(events, "cutflow.jet1_pt", Route("pt[:,0]").apply(selected_jet, EMPTY_FLOAT))
    events = set_ak_column_f32(events, "cutflow.jet1_eta", Route("eta[:,0]").apply(selected_jet, EMPTY_FLOAT))
    events = set_ak_column_f32(events, "cutflow.jet1_phi", Route("phi[:,0]").apply(selected_jet, EMPTY_FLOAT))
    events = set_ak_column_f32(events, "cutflow.jet2_pt", Route("pt[:,1]").apply(selected_jet, EMPTY_FLOAT))
    # btags
    btagDeepFlavB_padded = pad_events(events, "Jet.btagDeepFlavB", 2)
    btagDeepFlavCvB_padded = pad_events(events, "Jet.btagDeepFlavCvB", 2)

    events = set_ak_column_f32(events, "cutflow.btagDeepFlavB1", ak.sort(btagDeepFlavB_padded, axis=1, ascending=False)[:,0])
    events = set_ak_column_f32(events, "cutflow.btagDeepFlavCvB1", ak.sort(btagDeepFlavCvB_padded, axis=1, ascending=True)[:,0])
    
    events = set_ak_column_f32(events, "cutflow.btagDeepFlavB2", ak.sort(btagDeepFlavB_padded, axis=1, ascending=False)[:,1])
    events = set_ak_column_f32(events, "cutflow.btagDeepFlavCvB2", ak.sort(btagDeepFlavCvB_padded, axis=1, ascending=True)[:,1])
    
    

    # padded columns
    rawDeepTau2017v2p1VSe_padded = pad_events(events, "Tau.rawDeepTau2017v2p1VSe", 2)
    rawDeepTau2017v2p1VSjet_padded = pad_events(events, "Tau.rawDeepTau2017v2p1VSjet", 2)
    rawDeepTau2017v2p1VSmu_padded = pad_events(events, "Tau.rawDeepTau2017v2p1VSmu", 2)
    btagHbb_padded = pad_events(events, "FatJet.btagHbb", 2)
    btagDeepb_padded = pad_events(events, "FatJet.btagDeepB", 2)
    
    # deeptau
    events = set_ak_column_f32(events, "cutflow.rawDeepTau2017v2p1VSe1", ak.sort(rawDeepTau2017v2p1VSe_padded, axis=1, ascending=False)[:,0])
    events = set_ak_column_f32(events, "cutflow.rawDeepTau2017v2p1VSjet1", ak.sort(rawDeepTau2017v2p1VSjet_padded, axis=1, ascending=False)[:,0])
    events = set_ak_column_f32(events, "cutflow.rawDeepTau2017v2p1VSmu1", ak.sort(rawDeepTau2017v2p1VSmu_padded, axis=1, ascending=False)[:,0])

    events = set_ak_column_f32(events, "cutflow.rawDeepTau2017v2p1VSe2", ak.sort(rawDeepTau2017v2p1VSe_padded, axis=1, ascending=False)[:,1])
    events = set_ak_column_f32(events, "cutflow.rawDeepTau2017v2p1VSjet2", ak.sort(rawDeepTau2017v2p1VSjet_padded, axis=1, ascending=False)[:,1])
    events = set_ak_column_f32(events, "cutflow.rawDeepTau2017v2p1VSmu2", ak.sort(rawDeepTau2017v2p1VSmu_padded, axis=1, ascending=False)[:,1])
    # fatjets
    events = set_ak_column_f32(events, "cutflow.fatJet1.btagHbb", ak.sort(btagHbb_padded, axis=1, ascending=False)[:,0])
    events = set_ak_column_f32(events, "cutflow.fatJet1.btagDeepb", ak.sort(btagDeepb_padded, axis=1, ascending=False)[:,0])
    events = set_ak_column_f32(events, "cutflow.fatJet2.btagHbb", ak.sort(btagHbb_padded, axis=1, ascending=False)[:,1])
    events = set_ak_column_f32(events, "cutflow.fatJet2.btagDeepb", ak.sort(btagDeepb_padded, axis=1, ascending=False)[:,1])


    events = self[fatjet_tagging_variables](events)
    events = set_ak_column_f32(events, "cutflow.HardestFatJet.particleNet_HbbvsQCD", events.HardestFatJet.particleNet_HbbvsQCD)
    events = set_ak_column_f32(events, "cutflow.HardestFatJet.particleNetMD_Xbb", events.HardestFatJet.particleNet_HbbvsQCD)
    events = set_ak_column_f32(events, "cutflow.HardestFatJet.pt", events.HardestFatJet.particleNet_HbbvsQCD)

    return events