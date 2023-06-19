"""
Boosted Jet (Fatjet) selection methods.
"""

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import, dev_sandbox
from collections import defaultdict, OrderedDict

np = maybe_import("numpy")
ak = maybe_import("awkward")

@selector(
    uses={
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.jetId", "Jet.puId",
        "nFatJet", "FatJet.pt", "FatJet.eta", "FatJet.phi", "FatJet.mass", "FatJet.msoftdrop",
        "FatJet.jetId", "FatJet.subJetIdx1", "FatJet.subJetIdx2",
        "nSubJet", "SubJet.pt", "SubJet.eta", "SubJet.phi", "SubJet.mass", "SubJet.btagDeepB",
    },
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def boosted_jet_selector(
    self: Selector,
    events: ak.Array,
    trigger_results: SelectionResult,
    lepton_results: SelectionResult,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """Boosted jet selector, as an extension of the currently used
    jet selector.

    :param events: all events
    :param trigger_results: Results from trigger selection.
    :param lepton_results: Results from lepton selection.
    :return: events
    """
    # check whether the two bjets were matched by fatjet subjets to mark it as boosted
    fatjet_mask = (
        (events.FatJet.jetId == 6) &  # tight plus lepton veto
        (events.FatJet.msoftdrop > 30.0) &
        (abs(events.FatJet.eta) < 2.4) &
        ak.all(events.FatJet.metric_table(lepton_results.x.lepton_pair) > 0.5, axis=2) &  # metric_table = delta R
        (events.FatJet.subJetIdx1 >= 0) &
        (events.FatJet.subJetIdx2 >= 0)
    )

    fatjet_indices = ak.local_index(events.FatJet.pt)[fatjet_mask]
    # sorted_indices = ak.argsort(events.FatJet.pt, axis=-1, ascending=False)
    # fatjet_indices = sorted_indices[fatjet_mask[sorted_indices]]

    # TODO: change the event selection according to histograms
    fatjet_event_selection=(
        (ak.sum(fatjet_mask, axis=1) >= 1)
    )


    # Jet mask for the btag_weights, NOT USED FOR SELECTION
    is_2016 = self.config_inst.campaign.x.year == 2016

    ak4_mask = (
        (events.Jet.jetId == 6) &  # tight plus lepton veto
        ((events.Jet.pt >= 50.0) | (events.Jet.puId == (1 if is_2016 else 4))) &  # flipped in 2016
        ak.all(events.Jet.metric_table(lepton_results.x.lepton_pair) > 0.5, axis=2)
    )

    # default jets
    default_jet_mask = (
        ak4_mask &
        (events.Jet.pt > 20.0) &
        (abs(events.Jet.eta) < 2.4)
    )

    return events, SelectionResult(
        steps={
            "fatjet": fatjet_event_selection,
         },
        objects={
            "Jet": {
                # "Jet": jet_indices,
                # "HHBJet": hhbjet_indices,
                # "NonHHBJet": non_hhbjet_indices,
                "FatJet": fatjet_indices,
                # "SubJet1": subjet_indices[..., 0],
                # "SubJet2": subjet_indices[..., 1],
                # "VBFJet": vbfjet_indices,
            },
        },
        aux={
            # jet mask that leads to the jet_indices
            "fatjet_mask": fatjet_mask,
            "jet_mask": default_jet_mask,
            # used to determine sum of weights in increment_stats
            # "n_central_jets": ak.num(jet_indices, axis=1),
        },
    )
