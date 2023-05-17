# coding: utf-8

"""
Producers that determine the generator-level particles related to a top quark decay.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import EMPTY_FLOAT, layout_ak_array
from columnflow.columnar_util import set_ak_column


ak = maybe_import("awkward")

@producer(
        uses={
            # nano columns
            "event",
            "isHardProcess",
            "nGenPart", "GenPart.*",
        },
        produces={
            "genBpartonH", "genTaupartonH",
        },
        sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def gen_HH_decay_products(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates a new ragged column "gen_top_decay" with one element per hard top quark. Each element is
    a GenParticleArray with five or more objects in a distinct order: top quark, bottom quark,
    W boson, down-type quark or charged lepton, up-type quark or neutrino, and any additional decay
    produces of the W boson (if any, then most likly photon radiations). Per event, the structure
    will be similar to:

    .. code-block:: python

        [[t1, b1, W1, q1/l, q2/n(, additional_w_decay_products)], [t2, ...], ...]
    """

    def find_partons(events: ak.Array, pdgId: int, mother_pdgId: int=25):
        abs_id = abs(events.GenPart.pdgId)
        part = events.GenPart[abs_id == pdgId]
        part = part[part.hasFlags("isHardProcess")& (abs(part.distinctParent.pdgId) == mother_pdgId)]
        part = part[~ak.is_none(part, axis=1)]

        return part
    # from IPython import embed; embed()
    
    gen_b_from_h = find_partons(events=events, pdgId=5)
    gen_tau_from_h = find_partons(events=events, pdgId=15)
    # save the column
    # events = set_ak_column(events, "gen_top_decay", groups)
    events = set_ak_column(events, "genBpartonH", gen_b_from_h)
    events = set_ak_column(events, "genTaupartonH", gen_tau_from_h)
    return events

@producer(
        uses={
            # nano columns
            "event",
            "isHardProcess",
            "nGenPart", "GenPart.*",
            "nGenJet", "GenJet.*",
        },
        produces={
            "genBpartonH", "genTaupartonH",
        },
        sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def create_genbjets(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates gen jets with correct flavour.
    """
    genBjets = events.GenJet[abs(events.GenJet.partonFlavour) == 5]
    events = set_ak_column(events, "genBjets", genBjets)
    return events

@gen_HH_decay_products.init
def gen_HH_decay_products_init(self: Producer) -> None:
    """
    Ammends the set of used and produced columns of :py:class:`gen_top_decay_products` in case
    a dataset including top decays is processed.
    """
    self.uses |= {"nGenPart", "GenPart.*", "nGenJet", 'GenJet.*'}
    self.produces |= {"genBpartonH", "genTaupartonH"}
