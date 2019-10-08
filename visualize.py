#!/usr/bin/env python
# coding: utf-8
import os
import sys

import argparse
parser = argparse.ArgumentParser(description="Make GEN plots from NanoAOD")
parser.add_argument("files", type=str, nargs="+", help="File(s) to process")
parser.add_argument("--name", type=str, help="Sample name (=output folder name)")
args = parser.parse_args()

#if os.path.isdir(args.name):
#    raise ValueError("Output directory {} already exists.".format(args.name))
os.system("mkdir -pv {}".format(args.name))

import json
import uproot
import uproot_methods
import awkward
import numpy as np
from functools import partial
#get_ipython().run_line_magic('matplotlib', 'nbagg')

import coffea.processor as processor
import coffea.hist as hist
from coffea.util import load, save
import matplotlib.pyplot as plt

plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 14,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'xtick.direction': 'in',
    'xtick.top': True,
    'ytick.labelsize': 12,
    'ytick.direction': 'in',
    'ytick.right': True,
    'legend.fontsize': 12,
})

#f = uproot.open("RunIIAutumn18NanoAODv5_ZPrimeToQQ_DMsimp_HT400_M125_v4.root")
#t = f["Events"]


# In[2]:

def nanoObjectx(df, prefix):
        branches = set(k.decode('ascii') for k in df.available if k.decode('ascii').startswith(prefix))
        p4branches = [prefix + k for k in ['pt', 'eta', 'phi', 'mass']]
        branches -= set(p4branches)
        objp4 = uproot_methods.TLorentzVectorArray.from_ptetaphim(*[df[b] for b in p4branches])
        branches = {k[len(prefix):]: df[k] for k in branches}
        obj = awkward.JaggedArray.zip(p4=objp4, **branches)
        return obj

# What's in the file
from coffea.processor.dataframe import LazyDataFrame
# df = LazyDataFrame(t, 500000)
# #for ievent in range(len(df["GenPart_pdgId"])):
# #    print([i for i in zip(df["GenPart_pdgId"][ievent], df["GenPart_status"][ievent]) if i[0]==55])
# df["genWeight"]
# df["GenPart_pt"]
# genp = nanoObjectx(df, 'GenPart_')
# zp_index = (genp['pdgId']==55) & (genp['status']==62)
# print((genp['pdgId']==55) & (genp['status']==62))
# zp = genp[zp_index]   
# print(zp.p4.pt.flatten())

class GenVisualizer(processor.ProcessorABC):
    _gen_statusFlags = {
        0: 'isPrompt',
        1: 'isDecayedLeptonHadron',
        2: 'isTauDecayProduct',
        3: 'isPromptTauDecayProduct',
        4: 'isDirectTauDecayProduct',
        5: 'isDirectPromptTauDecayProduct',
        6: 'isDirectHadronDecayProduct',
        7: 'isHardProcess',
        8: 'fromHardProcess',
        9: 'isHardProcessTauDecayProduct',
        10: 'isDirectHardProcessTauDecayProduct',
        11: 'fromHardProcessBeforeFSR',
        12: 'isFirstCopy',
        13: 'isLastCopy',
        14: 'isLastCopyBeforeFSR',
        62: 'whatever',
    }

    def __init__(self):
        dataset_axis = hist.Cat("dataset", "Primary dataset")
        mass_axis = hist.Bin("mass", r"$m$", 50, 0., 500.)
        pt_axis = hist.Bin("pt", r"$p_{T,h}$ [GeV]", 120, 0., 1200.)
        
        self._accumulator = processor.dict_accumulator({
            "hmass":hist.Hist("Counts", dataset_axis, mass_axis),
            'hpt': hist.Hist("Counts", dataset_axis, pt_axis),
        })

    @property
    def accumulator(self):
        return self._accumulator

    def statusmask(self, array, require):
        mask = sum((1<<k) for k,v in self._gen_statusFlags.items() if v in require)
        return (array & mask)==mask

    def nanoObject(self, df, prefix):
        branches = set(k.decode('ascii') for k in df.available if k.decode('ascii').startswith(prefix))
        p4branches = [prefix + k for k in ['pt', 'eta', 'phi', 'mass']]
        branches -= set(p4branches)
        objp4 = uproot_methods.TLorentzVectorArray.from_ptetaphim(*[df[b] for b in p4branches])
        branches = {k[len(prefix):]: df[k] for k in branches}
        obj = awkward.JaggedArray.zip(p4=objp4, **branches)
        return obj

    def process(self, df):
        output = self.accumulator.identity()
        dataset = df['dataset']

        genp = self.nanoObject(df, 'GenPart_')
        zp_index = (genp['pdgId']==55) & (genp['status'] == 62)
        zp = genp[zp_index]   
        output['hmass'].fill(dataset=dataset,
                           mass=zp.p4.mass.flatten())
                           #weight=df['genWeight'])
        output['hpt'].fill(dataset=dataset,
                           pt=zp.p4.pt.flatten())#,
                           #weight=df['genWeight'])
        print(output['hmass'].values())
        return output

    def postprocess(self, accumulator):
        return accumulator

samples = {args.name:args.files}
#with open('files_prev.json') as fin:
#    samples = json.load(fin)

output = processor.run_uproot_job(samples,
                                  treename='Events',
                                  processor_instance=GenVisualizer(),
                                  executor=processor.futures_executor,
                                  executor_args={'workers': 4},
                                  chunksize=500000,
                                 )
save(output, '{}/genstuff.coffea'.format(args.name))

output = load("{}/genstuff.coffea".format(args.name))

#hmass = output["hmass"]
#bin_contents = hmass.values()[('ZPrimeToQQ_DMsimp_HT400_M50',)]
#edges = hmass.axis('mass').edges()
#edge_pairs = [(edges[i], edges[i+1]) for i in range(len(edges)-1)]
#histd = zip(edge_pairs, bin_contents)
#for thing in histd:
#    print(thing)


# In[13]:

for hname, axisname in [("hmass", "mass"), ("hpt", "pt")]:
    print(hname)
    hists = {k[0]: v for k,v in output[hname].values(sumw2=True, overflow='over').items()}
    fig, ax = plt.subplots()
    edges = output[hname].axis(axisname).edges()
    ax.step(x=edges[:], y=hists[args.name][0])
    fig.savefig("{}/{}.png".format(args.name, axisname))
