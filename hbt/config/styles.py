# coding: utf-8

"""
Style definitions.
"""

import order as od


def stylize_processes(config: od.Config) -> None:
    """
    Adds process colors and adjust labels.
    """
    if config.has_process("hh_ggf_bbtautau"):
        config.processes.n.hh_ggf_bbtautau.color1 = (67, 118, 201)

    config.skip_ratio = True
    # process colours for my datasets
    # as I do not plot any background, I can just use the whole colour range
    if config.has_process("graviton_hh_ggf_bbtautau_m400"):
        config.processes.n.graviton_hh_ggf_bbtautau_m400.color1 = (0,0,0)
        config.processes.n.graviton_hh_ggf_bbtautau_m400.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m400.label = "m=400"

    if config.has_process("graviton_hh_ggf_bbtautau_m800"):
        config.processes.n.graviton_hh_ggf_bbtautau_m800.color1 = (230,159,0)
        config.processes.n.graviton_hh_ggf_bbtautau_m800.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m800.label = "m=800"

    if config.has_process("graviton_hh_ggf_bbtautau_m1000"):
        config.processes.n.graviton_hh_ggf_bbtautau_m1000.color1 = (86,180,233)
        config.processes.n.graviton_hh_ggf_bbtautau_m1000.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m1000.label = "m=1000"

    if config.has_process("graviton_hh_ggf_bbtautau_m1250"):
        config.processes.n.graviton_hh_ggf_bbtautau_m1250.color1 = (0,158,115)
        config.processes.n.graviton_hh_ggf_bbtautau_m1250.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m1250.label = "m=1250"

    if config.has_process("graviton_hh_ggf_bbtautau_m1750"):
        config.processes.n.graviton_hh_ggf_bbtautau_m1750.color1 = (240,228,66)
        config.processes.n.graviton_hh_ggf_bbtautau_m1750.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m1750.label = "m=1750"

    if config.has_process("graviton_hh_ggf_bbtautau_m2000"):
        config.processes.n.graviton_hh_ggf_bbtautau_m2000.color1 = (0,114,178)
        config.processes.n.graviton_hh_ggf_bbtautau_m2000.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m2000.label = "m=2000"

    if config.has_process("graviton_hh_ggf_bbtautau_m2500"):
        config.processes.n.graviton_hh_ggf_bbtautau_m2500.color1 = (213,94,0)
        config.processes.n.graviton_hh_ggf_bbtautau_m2500.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m2500.label = "m=2500"

    if config.has_process("graviton_hh_ggf_bbtautau_m3000"):
        config.processes.n.graviton_hh_ggf_bbtautau_m3000.color1 = (204,121,167)
        config.processes.n.graviton_hh_ggf_bbtautau_m3000.unstack = True
        config.processes.n.graviton_hh_ggf_bbtautau_m3000.label = "m=3000"


    if config.has_process("h"):
        config.processes.n.h.color1 = (65, 180, 219)

    if config.has_process("tt"):
        config.processes.n.tt.color1 = (244, 182, 66)

    if config.has_process("st"):
        config.processes.n.st.color1 = (244, 93, 66)

    if config.has_process("dy"):
        config.processes.n.dy.color1 = (68, 186, 104)

    if config.has_process("vv"):
        config.processes.n.vv.color1 = (2, 24, 140)

    if config.has_process("qcd"):
        config.processes.n.qcd.color1 = (242, 149, 99)
