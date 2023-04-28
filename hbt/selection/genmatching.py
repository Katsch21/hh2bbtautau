"""
Gen matching selection methods.
"""

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import set_ak_column

from collections import defaultdict, OrderedDict
from IPython import embed

from hbt.production.gen_HH_decay import gen_HH_decay_products


np = maybe_import("numpy")
ak = maybe_import("awkward")

@selector(
    uses={
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.jetId", "Jet.puId",
        "nGenJet", "GenJet.*",
        "nGenVisTau", "GenVisTau.*", "Jet.genJetIdx", "Tau.genPartIdx",
        gen_HH_decay_products.PRODUCES,
    },
    produces={
        "GenmatchedJets", "GenmatchedHHBtagJets", "GenBPartons",
    },
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def genmatching_selector(
    self: Selector,
    events: ak.Array,
    jet_collection: ak.Array, # different collections can be matched now
    jet_results: SelectionResult,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    
    # match genJets to genPartons from H
    # get GenJets with b as partonFlavour
    genBjets = events.GenJet[abs(events.GenJet.partonFlavour) == 5]

    # Matching step 1 begins here!
    # calculate deltaR between genBjets and gen b partons from H
    nearest_genjet_to_parton = events.genBpartonH.nearest(genBjets, threshold=0.4)

    # filter unmatched cases
    unmatched_genjets = ak.is_none(nearest_genjet_to_parton.pt, axis=1)
    matched_genjets_to_parton = nearest_genjet_to_parton[~unmatched_genjets]

    # Matching step 2 begins here!
    # calculated deltaR between matched Gen Jets and Jets
    nearest_jets_to_genjets = matched_genjets_to_parton.nearest(jet_collection, threshold=0.4)
    
    # calculation of the indices of the matched jets
    metrics_jets_to_genjets = matched_genjets_to_parton.metric_table(jet_collection, axis=1)
    mmin = ak.argmin(metrics_jets_to_genjets, axis=2, keepdims=True)
    metric = ak.firsts(metrics_jets_to_genjets[mmin], axis=2)
    
    mmin = ak.firsts(mmin.mask[metric <= 0.4], axis=2)

     # filter unmatched cases
    unmatched_jets = ak.is_none(nearest_jets_to_genjets.pt, axis=1)

    # genmatched jets (not the indices, but the objects):
    matched_jets_to_genjets = nearest_jets_to_genjets[~unmatched_jets]

    # prepare HHBtagged Jet indices for the comparison with genmatchedJets:
    selected_hhbjet_indices=ak.pad_none(jet_results.objects.Jet.HHBJet,2,axis=1)
    padded_mmin=ak.pad_none(mmin,2,axis=1)

    # Comparison: HHBtagged Jets and Genmatched Jets: selection and matching begins here!
    # each genjet has (at least) one btag jet
    matched_and_selected = ak.from_iter([np.isin(selected_hhbjet_indices[index], padded_mmin[index]) for index in range(len(padded_mmin))])

    # event selection:
    at_least_one_jet_matched_event_selection=(
        (ak.sum(matched_and_selected, axis=1) >= 1)
    )

    first_jet_matched_event_selection=(
        matched_and_selected[:,0]
    )

    two_jet_matched_event_selection=(
        (ak.sum(matched_and_selected, axis=1) == 2)
    )

    # get indices for plotting genmatching steps:
    
    def find_genjet_indices(array1: ak.Array, array2: ak.Array):
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
        genjet_indices = ak.firsts(minimum_deltar_indices.mask[metric <= 0.4], axis=2)

        return genjet_indices
    
    # TODO: are genmatchedjets_indices and mmin the same?? Does the function work correctly?
    genmatchedgenjets_indices = find_genjet_indices(array1 = events.genBpartonH, array2 = genBjets)
    genmatchedjets_indices = find_genjet_indices(array1 = matched_genjets_to_parton, array2 = jet_collection)

    def find_partons_id(events: ak.Array, pdgId: int, mother_pdgId: int=25):
        abs_id = abs(events.GenPart.pdgId)
        part_id=ak.local_index(events.GenPart, axis=1)
        part = events.GenPart[abs_id == pdgId]
        part_id = part_id[abs_id == pdgId]
        part_id = part_id[part.hasFlags("isHardProcess")& (abs(part.distinctParent.pdgId) == mother_pdgId)]
        part = part[part.hasFlags("isHardProcess")& (abs(part.distinctParent.pdgId) == mother_pdgId)]
        part_id = part_id[~ak.is_none(part, axis=1)]
        return part_id

    part_id = find_partons_id(events, pdgId=5, mother_pdgId=25)

    # embed()
    
    # events = set_ak_column(events, "GenmatchedJets", events.Jet[mmin])
    # events = set_ak_column(events, "GenmatchedHHBtagJets", events.Jet[selected_hhbjet_indices][matched_and_selected])

    print("genmatching_done")

    return events, SelectionResult(
    steps={
        # Gen Matching Steps
        "gen_matched_1":at_least_one_jet_matched_event_selection,
        "first_matched":first_jet_matched_event_selection,
        "gen_matched_2":two_jet_matched_event_selection,
        },
    objects={
        "GenPart":{
            "GenBPartons": part_id, # Gen Partons (before matching)
        },
        "GenJet":{
            "GenmatchedGenJets": genmatchedgenjets_indices # Gen Jets (Matching step 1)
        },
        "Jet":{ 
            "GenmatchedJets": mmin, # detector jets (Matching Step 2)
            "GenmatchedHHBtagJets": matched_and_selected,
        }
        }
    )