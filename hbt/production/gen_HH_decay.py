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


@gen_HH_decay_products.init
def gen_HH_decay_products_init(self: Producer) -> None:
    """
    Ammends the set of used and produced columns of :py:class:`gen_top_decay_products` in case
    a dataset including top decays is processed.
    """
    self.uses |= {"nGenPart", "GenPart.*", "nGenJet", 'GenJet.*'}
    self.produces |= {"genBpartonH.*", "genTaupartonH.*"}

@producer(
        uses={
            # nano columns
            "event",
            "isHardProcess",
            "nGenPart", "GenPart.*",
        },
        sandbox=dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar.sh"),
)
def gen_HH_decay_product_idx(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
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
        # get all GenPart indices
        idx = ak.local_index(events.GenPart, axis=1)

        # filter requirements for interesting partons
        abs_id = abs(events.GenPart.pdgId)
        mask = (
            (abs_id == pdgId) &
            events.GenPart.hasFlags("isHardProcess") &
            (abs(events.GenPart.distinctParent.pdgId) == mother_pdgId)
        )
        
        # fill None values with False
        mask = ak.fill_none(mask, False)
        idx = idx[mask]
        # from IPython import embed
        # embed()
        return idx
    # from IPython import embed; embed()
    
    return find_partons(events, 5, 25)

