"""
Selection methods for a AK8 (FatJet) Gen Matching.
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
        "AK8_GenmatchedJets", "AK8_GenmatchedHHBtagJets", "AK8_GenBPartons",
    },
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def fatjet_genmatching_selector(
    self: Selector,
    events: ak.Array,
    jet_collection: ak.Array, # different collections can be matched now
    jet_results: SelectionResult,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    



    return events, SelectionResult(
    steps={
        # Gen Matching Steps
        # "gen_matched_1":at_least_one_jet_matched_event_selection,
        # "first_matched":first_jet_matched_event_selection,
        # "gen_matched_2":two_jet_matched_event_selection,
        },
    objects={
        # "GenPart":{
        #     "GenBPartons": part_id, # Gen Partons (before matching)
        # },
        # "GenJet":{
        #     "GenmatchedGenJets": genmatchedgenjets_indices # Gen Jets (Matching step 1)
        # },
        # "Jet":{ 
        #     "GenmatchedJets": mmin, # detector jets (Matching Step 2)
        #     "GenmatchedHHBtagJets": matched_and_selected,
        # }
        }
    )
    