# coding: utf-8

"""
Creates columns for different AK8 Jet discriminators X:
    HardestFatJet.X: Creates columns for discriminator value of hardest jet
    FirstFatJet.X: Creates columns for highest discriminator value 
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import EMPTY_FLOAT, layout_ak_array, Route
from columnflow.columnar_util import set_ak_column

from IPython import embed


ak = maybe_import("awkward")

@producer(
    fatjet_observables = {
        "btagCSVV2", "btagDeepB", "pt",
        "deepTagMD_H4qvsQCD",
        "deepTagMD_HbbvsQCD", "deepTagMD_ZHbbvsQCD", "deepTag_H",
        "particleNetMD_Xbb", "particleNet_HbbvsQCD", "btagHbb",
    },
    four_momenta_vars = {"pt", "phi", "eta", "mass"},
    sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def fatjet_tagging_variables(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates columns for HardestFatJet.X and FirstFatJet.X
    with X being the different tagging algorithms used fot H-> bb tagging,
    HardestFatJet being the hardest AK8-jet and FirstFatjet being the AK8-jet
    with the highest discriminator value.

    :param events: all events
    :return: events with new columns
        HardestFatJet.X with X being the tagging algorithms
        FirstFatjet.X with X being the tagging algorithms
        HardestFatjet.Y with Y being pT, eta, phi
        FirstFatjet.X.Y which corresponds to pT, eta, phi for
        AK8-jet with highest discriminator value
    """
    # sort for fatjet pt
    
    pt_indices = ak.argsort(events.FatJet.pt, axis=1, ascending=False)
    # embed()
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
        for four_mom_var in self.four_momenta_vars:
            events = set_ak_column(
                events,
                f"FirstFatJet_{var}.{four_mom_var}",
                Route(f"{four_mom_var}[:,0]").apply(events.FatJet[var_indices], EMPTY_FLOAT))
    
    for var in self.four_momenta_vars:
        events = set_ak_column(
            events,
            f"HardestFatJet.{var}",
            Route(f"{var}[:,0]").apply(events.FatJet[pt_indices], EMPTY_FLOAT))

    return events


@fatjet_tagging_variables.init
def fatjet_tagging_variables_init(self: Producer) -> None:
    """
    Ammends the set of used and produced columns of :py:class:`gen_top_decay_products` in case
    a dataset including top decays is processed.
    """
    self.uses |= {f"FatJet.{x}" for x in self.fatjet_observables}
    self.uses |= {f"FatJet.{x}" for x in self.four_momenta_vars}
    #print("fatjet_tagging_init")
    #embed()
    self.produces |= {f"HardestFatJet.{x}" for x in self.fatjet_observables}
    self.produces |= {f"FirstFatJet.{x}" for x in self.fatjet_observables}
    self.produces |= {f"HardestFatJet.{x}" for x in self.four_momenta_vars}
    self.produces |= {f"FirstFatJet.{x}" for x in self.four_momenta_vars}

