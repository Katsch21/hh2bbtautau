# coding: utf-8

"""
Producers that determine the generator-level particles related to a top quark decay.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import EMPTY_FLOAT, layout_ak_array, Route
from columnflow.columnar_util import set_ak_column


ak = maybe_import("awkward")

@producer(
    fatjet_observables = {
        "btagCSVV2", "btagDeepB", "pt"
        # "deepTagMD_H4qvsQCD",
        # "deepTagMD_HbbvsQCD", "deepTagMD_ZHbbvsQCD", "deepTag_H",
        # "particleNetMD_Xbb", "particleNet_HbbvsQCD", "btagHbb"
    },
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def fatjet_tagging_variables(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates a new ragged column "gen_top_decay" with one element per hard top quark. Each element is
    a GenParticleArray with five or more objects in a distinct order: top quark, bottom quark,
    W boson, down-type quark or charged lepton, up-type quark or neutrino, and any additional decay
    produces of the W boson (if any, then most likly photon radiations). Per event, the structure
    will be similar to:

    """
    # sort for fatjet pt

    pt_indices = ak.argsort(events.FatJet.pt, axis=1, ascending=False)
    from IPython import embed
    embed()
    for var in self.fatjet_observables:
        var_indices = ak.argsort(Route(f"{var}[...]").apply(events.FatJet, EMPTY_FLOAT), ascending=False)
        events = set_ak_column(
            events,
            f"HardestFatJet.{var}",
            Route(f"{var}[:,0]").apply(events.FatJet[pt_indices], EMPTY_FLOAT))
        events = set_ak_column(
            events,
            f"FirstFatJet.{var}",
            Route(f"{var}[:,0]").apply(events.FatJet[var_indices], EMPTY_FLOAT))

    return events


@fatjet_tagging_variables.init
def fatjet_tagging_variables_init(self: Producer) -> None:
    """
    Ammends the set of used and produced columns of :py:class:`gen_top_decay_products` in case
    a dataset including top decays is processed.
    """
    self.uses |= {f"FatJet.{x}" for x in self.fatjet_observables}
    self.produces |= {f"HardestFatJet.{x}" for x in self.fatjet_observables}
    self.produces |= {f"FirstFatJet.{x}" for x in self.fatjet_observables}
