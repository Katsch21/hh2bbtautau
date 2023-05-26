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
from hbt.selection.fatjet import fatjet_selection
from hbt.selection.lepton import lepton_selection
from hbt.selection.trigger import trigger_selection
from hbt.selection.default import increment_stats
from hbt.production.gen_HH_decay import gen_HH_decay_products
# from hbt.selection.genmatching import genmatching_selector
from hbt.production.delta_r import delta_r, get_pt, genmatched_delta_r
from IPython import embed

np = maybe_import("numpy")

ak = maybe_import("awkward")



def print_efficiency(results, skip_steps_list1, skip_steps_list2, name_selection1, name_selection2):
    """
    calculate and print efficiency between two selections, given by the steps to skip in results
    """
    results_reduced1 = reduce(
        and_,
        [mask for step_name, mask in results.steps.items() if step_name not in skip_steps_list1],
    )
    results_reduced2 = reduce(
        and_,
        [mask for step_name, mask in results.steps.items() if step_name not in skip_steps_list2],
    )

    n_events_results1=ak.sum(results_reduced1, axis=0)
    n_events_results2=ak.sum(results_reduced2, axis=0)

    print("n_events",name_selection1,n_events_results1)
    print("n_events",name_selection2,n_events_results2)

    efficiency=n_events_results2/n_events_results1
    print("efficiency between",name_selection1,"and",name_selection2,efficiency)




@selector(
    uses={boosted_jet_selector, lepton_selection, trigger_selection,
        process_ids, increment_stats, attach_coffea_behavior,
        btag_weights, pu_weight, mc_weight, pdf_weights, murmuf_weights,
        cutflow_features, fatjet_selection,
        },
    produces={
        trigger_selection, lepton_selection, boosted_jet_selector,
        process_ids, increment_stats, attach_coffea_behavior,
        btag_weights, pu_weight, mc_weight, pdf_weights, murmuf_weights,
        cutflow_features, fatjet_selection,
    },
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar_tf.sh"),
    exposed=True,
)
def fatjet_boosted(
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
    events, fatjet_results = self[fatjet_selection](events, trigger_results, lepton_results, **kwargs)
    results += fatjet_results

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

    print_efficiency(results, skip_steps_list1=["fatjet_sel"], 
                     skip_steps_list2=[], name_selection1="lepton_selection", 
                     name_selection2="fatjet_selection")

    event_sel = reduce(and_, results.steps.values())
    results.main["event"] = event_sel

    results.steps.all_but_bjet = reduce(
        and_,
        [mask for step_name, mask in results.steps.items() if step_name != "bjet"],
    )

    print("CHECK FATJETS")
    from IPython import embed
    embed()

    # create process ids
    events = self[process_ids](events, **kwargs)

    # increment stats
    events = self[increment_stats](events, results, stats, **kwargs)

    # some cutflow features
    events = self[cutflow_features](events, results.objects, **kwargs)

    return events, results