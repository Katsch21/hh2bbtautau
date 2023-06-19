"""
Selection methods for a AK8 (FatJet) Gen Matching.
"""

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import set_ak_column

from collections import defaultdict, OrderedDict
from IPython import embed

from hbt.production.gen_HH_decay import gen_HH_decay_products, gen_HH_decay_product_idx


np = maybe_import("numpy")
ak = maybe_import("awkward")

@selector(
    uses={
        "FatJet.pt", "FatJet.eta", "FatJet.phi", "FatJet.jetId", "FatJet.puId",
        "nGenJetAK8", "GenJetAK8.*",
        "nGenVisTau", "GenVisTau.*", "Jet.genJetIdx", "Tau.genPartIdx",
        gen_HH_decay_products.PRODUCES, gen_HH_decay_product_idx,
    },
    produces={
        "AK8_GenmatchedJets", "AK8_GenmatchedHHBtagJets", "AK8_GenBPartons", "GenBpartons",
        "GenmatchedGenFatJets", "GenmatchedFatJets", "GenmatchedHBBFatJets", "GenmatchedXBBFatJets",
    },
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def fatjet_genmatching_selector(
    self: Selector,
    events: ak.Array,
    fatjet_results: SelectionResult,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    gen_b_parton_idx = self[gen_HH_decay_product_idx](events, **kwargs)

    def find_fatjet_indices(array1: ak.Array, array2: ak.Array, threshold: float):
        """
        calculates indices of jets of a specific genmatching step
        in which array1 is matched to array2.
        """
        # calculate delta R between jets:
        metrics_genjets = array1.metric_table(array2, axis=1)
        # get indices of minimum delta R value:
        minimum_deltar_indices = ak.argmin(metrics_genjets, axis=2, keepdims=True)
        # filter only indices of minimum delta R value:
        metric = ak.firsts(metrics_genjets[minimum_deltar_indices], axis=2)
        # get indices:
        genjet_indices = ak.firsts(minimum_deltar_indices.mask[metric <= threshold], axis=2)
        return genjet_indices

    genmatchedgenfatjets_indices_2_entries = find_fatjet_indices(array1 = events.GenPart[gen_b_parton_idx], array2 = events.GenJetAK8, threshold=0.8)
    genmatchedgenfatjets_indices=ak.mask(genmatchedgenfatjets_indices_2_entries[:,0],genmatchedgenfatjets_indices_2_entries[:,0]==genmatchedgenfatjets_indices_2_entries[:,1])[..., np.newaxis]
    genmatchedfatjets_indices = find_fatjet_indices(array1 = events.GenJetAK8[genmatchedgenfatjets_indices], array2 = events.FatJet, threshold=0.8)

    selected_fatjet_indices = fatjet_results.objects.FatJet.FatJet
    # embed()
    selected_hbb_tagged_fatjet_index = selected_fatjet_indices[ak.argsort(events.FatJet[selected_fatjet_indices].particleNet_HbbvsQCD, axis=1, ascending=False)]
    selected_xbb_tagged_fatjet_index = selected_fatjet_indices[ak.argsort(events.FatJet[selected_fatjet_indices].particleNetMD_Xbb, axis=1, ascending=False)]
    padded_selected_hbb_tagged_fatjet_index=ak.pad_none(selected_hbb_tagged_fatjet_index,1,axis=1)
    padded_selected_xbb_tagged_fatjet_index=ak.pad_none(selected_xbb_tagged_fatjet_index,1,axis=1)
    hbb_tagged_fatjet_index=padded_selected_hbb_tagged_fatjet_index[:, 0][..., np.newaxis]
    xbb_tagged_fatjet_index=padded_selected_xbb_tagged_fatjet_index[:, 0][..., np.newaxis]
    selected_and_matched_hbb=ak.fill_none(genmatchedfatjets_indices==hbb_tagged_fatjet_index, False)
    selected_and_matched_xbb=ak.fill_none(genmatchedfatjets_indices==xbb_tagged_fatjet_index, False)

    return events, SelectionResult(
    steps={
        #Gen Matching Steps
        # "gen_matched": ak.fill_none(ak.where(ak.firsts(genmatchedfatjets_indices)>=0, True, False), False),
        # "gen_matching_hbb": ak.firsts(selected_and_matched_hbb),
        # "gen_matching_xbb":ak.firsts(selected_and_matched_xbb),
        },
    objects={
        "GenPart":{
            "GenBpartons": gen_b_parton_idx, # Gen Partons (before matching)
        },
        "GenJetAK8":{
            "GenmatchedGenFatJets": genmatchedgenfatjets_indices # Gen Jets (Matching step 1)
        },
        "FatJet":{ 
            "GenmatchedFatJets": genmatchedfatjets_indices, # detector fatjets (Matching Step 2)
            "GenmatchedHBBFatJets": ak.mask(genmatchedfatjets_indices, selected_and_matched_hbb),
            "GenmatchedXBBFatJets": ak.mask(genmatchedfatjets_indices, selected_and_matched_xbb),
        },
        }
    )
    