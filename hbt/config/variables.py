# coding: utf-8

"""
Definition of variables.
"""

import order as od

from columnflow.columnar_util import EMPTY_FLOAT


def add_variables(config: od.Config) -> None:
    """
    Adds all variables to a *config*.
    """
    config.add_variable(
        name="event",
        expression="event",
        binning=(1, 0.0, 1.0e9),
        x_title="Event number",
        discrete_x=True,
    )
    config.add_variable(
        name="run",
        expression="run",
        binning=(1, 100000.0, 500000.0),
        x_title="Run number",
        discrete_x=True,
    )
    config.add_variable(
        name="lumi",
        expression="luminosityBlock",
        binning=(1, 0.0, 5000.0),
        x_title="Luminosity block",
        discrete_x=True,
    )
    config.add_variable(
        name="n_jet",
        expression="n_jet",
        binning=(11, -0.5, 10.5),
        x_title="Number of jets",
        discrete_x=True,
    )
    config.add_variable(
        name="n_hhbtag",
        expression="n_hhbtag",
        binning=(4, -0.5, 3.5),
        x_title="Number of HH b-tags",
        discrete_x=True,
    )
    config.add_variable(
        name="ht",
        binning=[0, 80, 120, 160, 200, 240, 280, 320, 400, 500, 600, 800],
        unit="GeV",
        x_title="HT",
    )
    config.add_variable(
        name="jet1_pt",
        expression="Jet.pt[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"Jet 1 $p_{T}$",
    )
    config.add_variable(
        name="jet1_eta",
        expression="Jet.eta[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(30, -3.0, 3.0),
        x_title=r"Jet 1 $\eta$",
    )
    config.add_variable(
        name="jet2_pt",
        expression="Jet.pt[:,1]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"Jet 2 $p_{T}$",
    )
    config.add_variable(
        name="met_phi",
        expression="MET.phi",
        null_value=EMPTY_FLOAT,
        binning=(33, -3.3, 3.3),
        x_title=r"MET $\phi$",
    )

    ###################################################################
    ###################################################################
    # TODO: add more variables that are important for my selections

    # invariant mass bjets:
    config.add_variable(
        name="HHBJet_dijet_mass",
        expression="HHBJet_dijet_mass",
        null_value=EMPTY_FLOAT,
        binning=(20, 0, 1000),
        x_title=r"DeepJet b+bb+lepb tag discriminator "
    )


    config.add_variable(
        name="GenmatchedFatJets",
        expression="GenmatchedFatJets",
        null_value=EMPTY_FLOAT,
        binning=(25, 0, 270),
        x_title=r"GenmatchedFatJets"
    )
    # fatjet tagger: highest pt
    config.add_variable(
        name="HardestFatJet.mass",
        expression="HardestFatJet.mass",
        null_value=EMPTY_FLOAT,
        binning=(25, 0, 270),
        x_title=r"Mass of hardest AK8 jet"
    )
    config.add_variable(
        name="HardestFatJet.btagHbb",
        expression="HardestFatJet.btagHbb",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"btagHbb AK8 jet discriminator value for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.btagCSVV2",
        expression="HardestFatJet.btagCSVV2",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"btagCSVV2 AK8 jet discriminator value for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.btagDeepB",
        expression="HardestFatJet.btagDeepB",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"btagDeepB AK8 jet discriminator value for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.deepTagMD_H4qvsQCD",
        expression="HardestFatJet.deepTagMD_H4qvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"deepTagMD_H4qvsQCD AK8 jet discr. val. for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.deepTagMD_HbbvsQCD",
        expression="HardestFatJet.deepTagMD_HbbvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"deepTagMD_HbbvsQCD AK8 jet discr. val. for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.deepTagMD_ZHbbvsQCD",
        expression="HardestFatJet.deepTagMD_ZHbbvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"deepTagMD_ZHbbvsQCD AK8 jet discr. val. for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.deepTag_H",
        expression="HardestFatJet.deepTag_H",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"deepTag_H AK8 jet discriminator value for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.particleNetMD_Xbb",
        expression="HardestFatJet.particleNetMD_Xbb",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"particleNetMD_Xbb AK8 jet discr. val. for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.particleNet_HbbvsQCD",
        expression="HardestFatJet.particleNet_HbbvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1),
        x_title=r"particleNet_HbbvsQCD AK8 jet discr. val. for hardest jet"
    )
    config.add_variable(
        name="HardestFatJet.pt",
        expression="HardestFatJet.pt",
        null_value=EMPTY_FLOAT,
        binning=(50, 0, 1000),
        x_title=r"pt of hardest jet"
    )

    ################################
    # fatjet tagger: sortiert nach observablen
    config.add_variable(
        name="FirstFatJet.btagHbb",
        expression="FirstFatJet.btagHbb",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discriminator value of btagHbb AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.btagCSVV2",
        expression="FirstFatJet.btagCSVV2",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discriminator value of btagCSVV2 AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.btagDeepB",
        expression="FirstFatJet.btagDeepB",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discriminator value of btagDeepB AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.deepTagMD_H4qvsQCD",
        expression="FirstFatJet.deepTagMD_H4qvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discr. val. of deepTagMD_H4qvsQCD AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.deepTagMD_HbbvsQCD",
        expression="FirstFatJet.deepTagMD_HbbvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discriminator value of HbbvsQCD AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.deepTagMD_ZHbbvsQCD",
        expression="FirstFatJet.deepTagMD_ZHbbvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discr. val. of deepTagMD_ZHbbvsQCD AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.deepTag_H",
        expression="FirstFatJet.deepTag_H",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discriminator value of deepTag_H AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.particleNetMD_Xbb",
        expression="FirstFatJet.particleNetMD_Xbb",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discr. val. of particleNetMD_Xbb AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.particleNet_HbbvsQCD",
        expression="FirstFatJet.particleNet_HbbvsQCD",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1),
        x_title=r"Highest discr. val. of particleNet_HbbvsQCD AK8 Jet tagger"
    )
    config.add_variable(
        name="FirstFatJet.pt",
        expression="FirstFatJet.pt",
        null_value=EMPTY_FLOAT,
        binning=(15, 0, 1000),
        x_title=r"pt of hardest jet"
    )
    #############################

    # btags:
    config.add_variable(
        name="jet1_btag_deepflavb",
        expression="Jet.btagDeepFlavB[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"DeepJet b+bb+lepb tag discriminator "
    )
    config.add_variable(
        name="jet_btag_deepflavcvb",
        expression="Jet.btagDeepFlavCvB[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"DeepJet c vs b+bb+lepb discriminator (inverted c tagger)"
    )
    ###################
    # delta R boosted:
    config.add_variable(
        name="delta_r_partons_boosted",
        expression="delta_r_partons_boosted",
        null_value=EMPTY_FLOAT,
        binning=(20, 0, 1.5),
        x_title=r"Delta R of two Gen Partons, boosted selection"
    )

    # delta R resolved:
    config.add_variable(
        name="delta_r_genbpartons",
        expression="delta_r_genbpartons",
        null_value=EMPTY_FLOAT,
        binning=(20, 0, 1.5),
        x_title=r"Delta R of two Gen Partons"
    )
    config.add_variable(
        name="delta_r_genmatchedgenjets",
        expression="delta_r_genmatchedgenjets",
        null_value=EMPTY_FLOAT,
        binning=(20, 0, 1.5),
        x_title=r"Delta R of two genmatched Gen Jets"
    )
    config.add_variable(
        name="delta_r_2_matches",
        expression="delta_r_2_matches",
        null_value=EMPTY_FLOAT,
        binning=(20, 0, 1.5),
        x_title=r"Delta R of two Gen matched jets"
    )
    config.add_variable(
        name="delta_r_HHbtag",
        expression="delta_r_HHbtag",
        null_value=EMPTY_FLOAT,
        binning=(20, 0, 1.5),
        x_title=r"Delta R of two Gen matched and HHbtag selected jets"
    )
    # pt of first matched jet:
    # 1D plots 20 bins, 2D plots 50 bins
    config.add_variable(
        name="first_pt_genbpartons",
        expression="first_pt_genbpartons",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"pt of first Gen Parton"
    )
    config.add_variable(
        name="first_pt_genmatchedgenjets",
        expression="first_pt_genmatchedgenjets",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"pt of first genmatched Gen Jet"
    )
    config.add_variable(
        name="first_pt_2_matches",
        expression="first_pt_2_matches",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"pt of first matched jet"
    )
    config.add_variable(
        name="first_pt_btag",
        expression="first_pt_btag",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"pt of first Gen matched and HHbtag selected jet"
    )
    # pt sum of both matched jets:
    config.add_variable(
        name="sum_pt_genbpartons",
        expression="sum_pt_genbpartons",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"sum: pt of first 2 Gen Partons"
    )
    config.add_variable(
        name="sum_pt_genmatchedgenjets",
        expression="sum_pt_genmatchedgenjets",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"sum: pt of first 2 genmatched Gen Jets"
    )
    config.add_variable(
        name="sum_pt_2_matches",
        expression="sum_pt_2_matches",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"sum: pt of first 2 matched jets"
    )
    config.add_variable(
        name="sum_pt_btag",
        expression="sum_pt_btag",
        null_value=EMPTY_FLOAT,
        binning=(20, 0.0, 1000.0),
        x_title=r"sum: pt of first 2 matched and selected jets"
    )

    # fatjets:
    config.add_variable(
        name="fatjet_mass",
        expression="FatJet.mass[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 500),
        x_title=r"Higgs to BB tagger discriminator"
    )
    config.add_variable(
        name="fatjet_btag_hbb",
        expression="FatJet.btagHbb[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"Higgs to BB tagger discriminator"
    )
    config.add_variable(
        name="fatjet_btag_deepb",
        expression="FatJet.btagDeepB",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"DeepCSV b+bb tag discriminator"
    )
    config.add_variable(
        name="fatjet_deeptag_h",
        expression="FatJet.deepTag.H[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"DeepBoostedJet tagger H(bb,cc,4q) sum"
    )
    config.add_variable(
        name="fatjet_deeptagmd_h4qvsqcd",
        expression="FatJet.deepTagMD.H4qvsQCD[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"Mass-decorrelated DeepBoostedJet tagger H->4q vs QCD discriminator"
    )
    config.add_variable(
        name="fatjet_deeptagmd_hbbvsqcd",
        expression="FatJet.deepTagMD.HbbvsQCD[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"Mass-decorrelated DeepBoostedJet tagger H->bb vs QCD discriminator"
    )
    config.add_variable(
        name="fatjet_particlenetmd_xbb",
        expression="FatJet.particleNetMD.Xbb[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"Mass-decorrelated ParticleNet tagger raw X->bb score"
    )
    config.add_variable(
        name="fatjet_particlenet_h4qvsqcd",
        expression="FatJet.particleNet.H4qvsQCD[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"ParticleNet tagger H(->VV->qqqq) vs QCD discriminator"
    )
    config.add_variable(
        name="fatjet_particlenet_hbbvsqcd",
        expression="FatJet.particleNet.HbbvsQCD[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"ParticleNet tagger H(->bb) vs QCD discriminator"
    )
    
    # deeptau:
    config.add_variable(
        name="tau_rawdeeptau2017v2p1_vs_e",
        expression="Tau.rawDeepTau2017v2p1VSe",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"byDeepTau2017v2p1VSe raw output discriminator"
    )
    config.add_variable(
        name="tau_rawdeeptau2017v2p1_vs_jet",
        expression="Tau.rawDeepTau2017v2p1VSjet",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"byDeepTau2017v2p1VSjet raw output discriminator"
    )
    config.add_variable(
        name="tau_rawdeeptau2017v2p1_vs_mu",
        expression="Tau.rawDeepTau2017v2p1VSmu",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"byDeepTau2017v2p1VSmu raw output discriminator"
    )

    #####

    config.add_variable(
        name="fatjet_pt",
        expression="FatJet.pt[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(500, 0, 500),
        x_title=r"AK8 Jet p_{T}"
    )


    # weights
    config.add_variable(
        name="mc_weight",
        expression="mc_weight",
        binning=(200, -10, 10),
        x_title="MC weight",
    )
    config.add_variable(
        name="pu_weight",
        expression="pu_weight",
        binning=(40, 0, 2),
        x_title="Pileup weight",
    )
    config.add_variable(
        name="normalized_pu_weight",
        expression="normalized_pu_weight",
        binning=(40, 0, 2),
        x_title="Normalized pileup weight",
    )
    config.add_variable(
        name="btag_weight",
        expression="btag_weight",
        binning=(60, 0, 3),
        x_title="b-tag weight",
    )
    config.add_variable(
        name="normalized_btag_weight",
        expression="normalized_btag_weight",
        binning=(60, 0, 3),
        x_title="Normalized b-tag weight",
    )
    config.add_variable(
        name="normalized_njet_btag_weight",
        expression="normalized_njet_btag_weight",
        binning=(60, 0, 3),
        x_title="$N_{jet}$ normalized b-tag weight",
    )

###################################################################
###################################################################
    # cutflow variables
    config.add_variable(
        name="cf_njet",
        expression="cutflow.n_jet",
        binning=(11, -0.5, 10.5),
        x_title="Jet multiplicity",
        discrete_x=True,
    )
    config.add_variable(
        name="cf_ht",
        expression="cutflow.ht",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"$H_{T}$",
    )
    config.add_variable(
        name="cf_jet1_pt",
        expression="cutflow.jet1_pt",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"Jet 1 $p_{T}$",
    )
    config.add_variable(
        name="cf_jet1_eta",
        expression="cutflow.jet1_eta",
        binning=(40, -5.0, 5.0),
        x_title=r"Jet 1 $\eta$",
    )
    config.add_variable(
        name="cf_jet1_phi",
        expression="cutflow.jet1_phi",
        binning=(32, -3.2, 3.2),
        x_title=r"Jet 1 $\phi$",
    )

    config.add_variable(
        name="cf_jet2_pt",
        expression="cutflow.jet2_pt",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"Jet 2 $p_{T}$",
    )

    # btags:
    config.add_variable(
        name="cf_btag1_deepflavb",
        expression="cutflow.btagDeepFlavB1",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"max(DeepJet b+bb+lepb tag discriminator)",
    )
    config.add_variable(
            name="cf_btag1_deepflavcvb",
            expression="cutflow.btagDeepFlavCvB1",
            null_value=EMPTY_FLOAT,
            binning=(100, 0, 1),
            x_title=r"max(inverted DeepJet c vs b+bb+lepb discriminator)"
        )
    config.add_variable(
        name="cf_btag2_deepflavb",
        expression="cutflow.btagDeepFlavB2",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"second highest DeepJet b+bb+lepb tag discriminator",
    )
    config.add_variable(
            name="cf_btag2_deepflavcvb",
            expression="cutflow.btagDeepFlavCvB2",
            null_value=EMPTY_FLOAT,
            binning=(100, 0, 1),
            x_title=r"second(inverted DeepJet c vs b+bb+lepb discriminator)"
        )
    
    # cutflow variables:
    # fatjet tagger:
    config.add_variable(
            name="cf_HardestFatJet.particleNet_HbbvsQCD",
            expression="cutflow.HardestFatJet.particleNet_HbbvsQCD",
            null_value=EMPTY_FLOAT,
            binning=(100, 0, 1),
            x_title=r"particleNet_HbbvsQCD discriminator of hardest fatjet"
        )
    config.add_variable(
            name="cf_HardestFatJet.particleNetMD_Xbb",
            expression="cutflow.HardestFatJet.particleNetMD_Xbb",
            null_value=EMPTY_FLOAT,
            binning=(100, 0, 1),
            x_title=r"particleNetMD_Xbb discriminator of hardest fatjet"
        )
    config.add_variable(
            name="cf_HardestFatJet.pt",
            expression="cutflow.HardestFatJet.pt",
            null_value=EMPTY_FLOAT,
            binning=(100, 0, 1),
            x_title=r"pt of hardest fatjet"
        )

    #deeptau:
    config.add_variable(
        name="cf_rawdeeptau2017v2p1_vs_e1",
        expression="cutflow.rawDeepTau2017v2p1VSe1",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"max of byDeepTau2017v2p1VSe raw output discriminator"
    )
    config.add_variable(
        name="cf_rawdeeptau2017v2p1_vs_jet1",
        expression="cutflow.rawDeepTau2017v2p1VSjet1",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"max of byDeepTau2017v2p1VSjet raw output discriminator"
    )
    config.add_variable(
        name="cf_rawdeeptau2017v2p1_vs_mu1",
        expression="cutflow.rawDeepTau2017v2p1VSmu1",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"max of byDeepTau2017v2p1VSmu raw output discriminator"
    )
    config.add_variable(
        name="cf_rawdeeptau2017v2p1_vs_e2",
        expression="cutflow.rawDeepTau2017v2p1VSe2",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"second highest byDeepTau2017v2p1VSe raw output discriminator"
    )
    config.add_variable(
        name="cf_rawdeeptau2017v2p1_vs_jet2",
        expression="cutflow.rawDeepTau2017v2p1VSjet2",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"second highest byDeepTau2017v2p1VSjet raw output discriminator"
    )
    config.add_variable(
        name="cf_rawdeeptau2017v2p1_vs_mu2",
        expression="cutflow.rawDeepTau2017v2p1VSmu2",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"second highest byDeepTau2017v2p1VSmu raw output discriminator"
    )

    # fatjets:
    config.add_variable(
        name="cf_fatjet_btag_hbb1",
        expression="cutflow.fatJet1.btagHbb",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"max of Higgs to BB tagger discriminator"
    )
    config.add_variable(
        name="cf_fatjet_btag_deepb1",
        expression="cutflow.fatJet1.btagDeepb",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"max of DeepCSV b+bb tag discriminator"
    )
    config.add_variable(
        name="cf_fatjet_btag_hbb2",
        expression="cutflow.fatJet2.btagHbb",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"second highest Higgs to BB tagger discriminator"
    )
    config.add_variable(
        name="cf_fatjet_btag_deepb2",
        expression="cutflow.fatJet2.btagDeepb",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 1),
        x_title=r"second highest DeepCSV b+bb tag discriminator"
    )
    
