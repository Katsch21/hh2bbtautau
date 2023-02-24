from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import, dev_sandbox
from columnflow.production.processes import process_ids

from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.cms.pileup import pu_weight
from columnflow.production.cms.pdf import pdf_weights
from columnflow.production.cms.scale import murmuf_weights
from columnflow.production.cms.btag import btag_weights
from columnflow.production.util import attach_coffea_behavior
from hbt.production.features import cutflow_features

from collections import defaultdict, OrderedDict

from operator import and_
from functools import reduce

from hbt.selection.boosted_jet import boosted_jet_selector
from hbt.selection.lepton import lepton_selection
from hbt.selection.trigger import trigger_selection
from hbt.selection.default import increment_stats
from hbt.production.gen_HH_decay import gen_HH_decay_products
from IPython import embed

np = maybe_import("numpy")

ak = maybe_import("awkward")


@selector(
    uses={boosted_jet_selector, lepton_selection, trigger_selection,
        process_ids, increment_stats, attach_coffea_behavior,
        btag_weights, pu_weight, mc_weight, pdf_weights, murmuf_weights,
        cutflow_features, gen_HH_decay_products, "nGenJet", "GenJet.*",
        "nGenVisTau", "GenVisTau.*"
          },
    produces={
        trigger_selection, lepton_selection, boosted_jet_selector,
        process_ids, increment_stats, attach_coffea_behavior,
        btag_weights, pu_weight, mc_weight, pdf_weights, murmuf_weights,
        cutflow_features,
    },
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
    exposed=True,
)
def boosted(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:

    # ensure coffea behavior
    events = self[attach_coffea_behavior](events, **kwargs)

    # add corrected mc weights
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    results = SelectionResult()
    # trigger selection
    events, trigger_results = self[trigger_selection](events, **kwargs)
    results += trigger_results

    # lepton selection
    events, lepton_results = self[lepton_selection](events, trigger_results, **kwargs)
    results += lepton_results

    # fat jet selections
    events, jet_results = self[boosted_jet_selector](events, trigger_results, lepton_results, **kwargs)
    results += jet_results

    # mc-only functions
    if self.dataset_inst.is_mc:
        # pdf weights
        events = self[pdf_weights](events, **kwargs)

        # renormalization/factorization scale weights
        events = self[murmuf_weights](events, **kwargs)

        # pileup weights
        events = self[pu_weight](events, **kwargs)

        # btag weights
        events = self[btag_weights](events, results.x.jet_mask, **kwargs)

    event_sel = reduce(and_, results.steps.values())
    results.main["event"] = event_sel

    results.steps.all_but_bjet = reduce(
        and_,
        [mask for step_name, mask in results.steps.items() if step_name != "bjet"],
    )

    # create process ids
    events = self[process_ids](events, **kwargs)

    # increment stats
    events = self[increment_stats](events, results, stats, **kwargs)

    # some cutflow features
    events = self[cutflow_features](events, **kwargs)

    events = self[gen_HH_decay_products](events, **kwargs)

    # match genJets to genPartons from H
    # get GenJets with b as partonFlavour
    genBjets = events.GenJet[abs(events.GenJet.partonFlavour) == 5]

    # calculate deltaR between genBjets and gen b partons from H
    metric = genBjets.metric_table(events.genBpartonH)
    from IPython import embed; embed()

    return events, results