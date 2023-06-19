# coding: utf-8

"""
Jet selection methods.
Every selection with TODO is changed to 0
so that the number of jets that pass the filter does not change.
"""

from operator import or_
from functools import reduce

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, Route, EMPTY_FLOAT

from hbt.production.gen_HH_decay import gen_HH_decay_product_idx
from IPython import embed

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={
        # hhbtag,
        # custom columns created upstream, probably by a selector
        "trigger_ids", "channel_id",
        # nano columns
        "nJet", "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.jetId", "Jet.puId",
        "Jet.btagDeepFlavB",
        "nFatJet", "FatJet.pt", "FatJet.eta", "FatJet.phi", "FatJet.mass", "FatJet.msoftdrop",
        "FatJet.jetId", "FatJet.subJetIdx1", "FatJet.subJetIdx2", "FatJet.particleNet_HbbvsQCD",
        "FatJet.*",
        "nSubJet", "SubJet.pt", "SubJet.eta", "SubJet.phi", "SubJet.mass", "SubJet.btagDeepB",
        # gen_HH_decay_product_idx,
    },
    
    # shifts are declared dynamically below in jet_selection_init
)
def fatjet_selection(
    self: Selector,
    events: ak.Array,
    trigger_results: SelectionResult,
    lepton_results: SelectionResult,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Jet selection based on ultra-legacy recommendations.

    Resources:
    https://twiki.cern.ch/twiki/bin/view/CMS/JetID?rev=107#nanoAOD_Flags
    https://twiki.cern.ch/twiki/bin/view/CMS/JetID13TeVUL?rev=15#Recommendations_for_the_13_T_AN1
    https://twiki.cern.ch/twiki/bin/view/CMS/PileupJetIDUL?rev=17
    https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookNanoAOD?rev=100#Jets
    """
    is_2016 = self.config_inst.campaign.x.year == 2016

    # local jet index
    li = ak.local_index(events.Jet)

    channelid = events.channel_id
    print(channelid)

    # common ak4 jet mask for normal and vbf jets
    ak4_mask = (
        (events.Jet.jetId == 6) &  # tight plus lepton veto
        ((events.Jet.pt >= 50.0) | (events.Jet.puId == (1 if is_2016 else 4))) &  # flipped in 2016
        ak.all(events.Jet.metric_table(lepton_results.x.lepton_pair) > 0.5, axis=2)
    )

    # default jets
    default_mask = (
        ak4_mask &
        (events.Jet.pt > 20.0) &
        (abs(events.Jet.eta) < 2.4)
    )

    # get indices for actual book keeping only for events with both lepton candidates and where at
    # most one jet pass the default mask 
    valid_score_mask = (
        default_mask &
        (ak.sum(default_mask, axis=1) <= 1) & ##  #in original paper : >=2 # TODO
        (ak.num(lepton_results.x.lepton_pair, axis=1) == 2)
    )


    # check whether the two bjets were matched by fatjet subjets to mark it as boosted
    vanilla_fatjet_mask = (
        (events.FatJet.jetId == 6) &  # tight plus lepton veto
        (events.FatJet.msoftdrop > 30.0) &
        (abs(events.FatJet.eta) < 2.4) &
        ak.all(events.FatJet.metric_table(lepton_results.x.lepton_pair) > 0.5, axis=2) &
        (events.FatJet.subJetIdx1 >= 0) &
        (events.FatJet.subJetIdx2 >= 0)
    )

    fatjet_mask = vanilla_fatjet_mask 

    # store fatjet and subjet indices
    ################# TODO define wanted subjets (and corresponding fatjets?)
    ########### high hbb tagging score?
    ########### two subjets, btagged 
    ########### both?
    ########### one or the other?
    fatjet_indices = ak.local_index(events.FatJet.pt)[fatjet_mask]
    subjet_indices = ak.concatenate(
        [
            events.FatJet[fatjet_mask].subJetIdx1[..., None],
            events.FatJet[fatjet_mask].subJetIdx2[..., None],
        ],
        axis=2,
    )

    # embed()
    # discard the event in case the (first) fatjet with matching subjets is found
    # but they are not b-tagged (TODO: move to deepjet when available for subjets)
    wp = self.config_inst.x.btag_working_points.deepcsv.loose
    subjets_btagged = ak.all(events.SubJet[ak.firsts(subjet_indices)].btagDeepB > wp, axis=1)

    ####################### TODO


    # pt sorted indices to convert mask
    sorted_indices = ak.argsort(events.Jet.pt, axis=-1, ascending=False)
    jet_indices = sorted_indices[default_mask[sorted_indices]]


    ############################### TODO define events selection through fatjets
    # final event selection
    fatjet_sel = (
        (ak.sum(default_mask, axis=1) <= 1) # & ## #in original paper : >=2 # TODO
        # ak.fill_none(subjets_btagged, True)  # was none for events with no matched fatjet
    )

    # STEPS FOR FIRST SELECTION BEGIN HERE
    # fatjet selection

    hbb_tagger_indices = ak.argsort(Route("particleNet_HbbvsQCD[...]").apply(events.FatJet[fatjet_mask], EMPTY_FLOAT), ascending=False)
    first_fatjet_hbb_tagger_score = Route("particleNet_HbbvsQCD[:,0]").apply(events.FatJet[fatjet_mask][hbb_tagger_indices], EMPTY_FLOAT)

    fatjet_sel_hbb_tagger_0_4 = (
        (ak.sum(default_mask, axis=1) <= 1) & ## #in original paper : >=2 # TODO
        (first_fatjet_hbb_tagger_score > 0.4)  # was none for events with no matched fatjet
    )

    fatjet_sel_hbb_tagger_0_6 = (
        (ak.sum(default_mask, axis=1) <= 1) & ## #in original paper : >=2 # TODO
        (first_fatjet_hbb_tagger_score > 0.6)  # was none for events with no matched fatjet
    )

    fatjet_sel_hbb_tagger_0_8 = (
        (ak.sum(default_mask, axis=1) <= 1) & ## #in original paper : >=2 # TODO
        (first_fatjet_hbb_tagger_score > 0.8)  # was none for events with no matched fatjet
    )
    # embed()

# STEPS FOR SECOND SELECTION BEGIN HERE
# subjet selection
    wp_loose = self.config_inst.x.btag_working_points.deepcsv.loose
    wp_medium = self.config_inst.x.btag_working_points.deepcsv.medium
    wp_tight = self.config_inst.x.btag_working_points.deepcsv.tight

    fatjet_subjet_tagging_sel_loose = (
        (ak.sum(default_mask, axis=1) <= 1) & ## #in original paper : >=2 # TODO
        ak.any((events.SubJet[events.FatJet[fatjet_mask].subJetIdx1].btagDeepB > wp_loose) & 
               (events.SubJet[events.FatJet[fatjet_mask].subJetIdx2].btagDeepB > wp_loose), axis=1)
               ####################### two subjets, btagged
    )
    fatjet_subjet_tagging_sel_medium = (
        (ak.sum(default_mask, axis=1) <= 1) & ## #in original paper : >=2 # TODO
        ak.any((events.SubJet[events.FatJet[fatjet_mask].subJetIdx1].btagDeepB > wp_medium) & 
               (events.SubJet[events.FatJet[fatjet_mask].subJetIdx2].btagDeepB > wp_medium), axis=1)
               ####################### two subjets, btagged
    )
    fatjet_subjet_tagging_sel_tight = (
        (ak.sum(default_mask, axis=1) <= 1) & ## #in original paper : >=2 # TODO
        ak.any((events.SubJet[events.FatJet[fatjet_mask].subJetIdx1].btagDeepB > wp_tight) & 
               (events.SubJet[events.FatJet[fatjet_mask].subJetIdx2].btagDeepB > wp_tight), axis=1)
               ####################### two subjets, btagged
    )
    # from IPython import embed; embed()
    ##############################  TODO

    # some final type conversions
    jet_indices = ak.values_astype(ak.fill_none(jet_indices, 0), np.int32)
    fatjet_indices = ak.values_astype(fatjet_indices, np.int32)

    # store some columns
    # events = set_ak_column(events, "Jet.hhbtag", hhbtag_scores)


    #### TEMPORARY FIX: define gen b parton collection here
    # gen_b_parton_idx = self[gen_HH_decay_product_idx](events, **kwargs)
    # build and return selection results plus new columns (src -> dst -> indices)
    return events, SelectionResult(
        steps={
            # always include to get practical cutflow plots
            "fatjet_sel": fatjet_sel,
            #
            # version b
            # "fatjet_sel_hbb_tagger_0_4": fatjet_sel_hbb_tagger_0_4,
            # "fatjet_sel_hbb_tagger_0_6": fatjet_sel_hbb_tagger_0_6,
            # "fatjet_sel_hbb_tagger_0_8": fatjet_sel_hbb_tagger_0_8,
            #
            # version a
            # "fatjet_subjet_tagging_sel_loose": fatjet_subjet_tagging_sel_loose,
            # "fatjet_subjet_tagging_sel_medium": fatjet_subjet_tagging_sel_medium,
            # "fatjet_subjet_tagging_sel_tight": fatjet_subjet_tagging_sel_tight,
            
            # "vanilla_fatjet": vanilla_fatjet_mask,
            # "subjet_match": subjets_match,
            # "fatjet_mask": fatjet_mask,
            # "hhbtag_validscore": valid_score_mask,
            # the btag weight normalization requires a selection with everything but the bjet
            # selection, so add this step here
            # note: there is currently no b-tag discriminant cut at this point, so take jet_sel
            # "fatjet_tagging": fatjet_tagging_sel,
        },
        objects={
            "Jet": {
                "Jet": jet_indices,
                ############ TODO Field FatJet with two btagged subjets
            },
            "FatJet": {
                "FatJet": fatjet_indices,
            },
            "SubJet": {
                "SubJet1": subjet_indices[..., 0],
                "SubJet2": subjet_indices[..., 1],
            },
            # "GenPart": {
            #     "GenBpartonH": gen_b_parton_idx,
            # },
        },
        aux={
            # jet mask that lead to the jet_indices
            "jet_mask": default_mask,
            "fatjet_mask": fatjet_mask,
            # used to determine sum of weights in increment_stats
            "n_central_jets": ak.num(jet_indices, axis=1),
        },
    )


@fatjet_selection.init
def fatjet_selection_init(self: Selector) -> None:
    # register shifts
    self.shifts |= {
        shift_inst.name
        for shift_inst in self.config_inst.shifts
        if shift_inst.has_tag(("jec", "jer"))
    }
