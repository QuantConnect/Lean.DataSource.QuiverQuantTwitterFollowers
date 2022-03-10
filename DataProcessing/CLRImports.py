from os import remove
from json import dump
from clr_loader import get_coreclr
from pythonnet import set_runtime

with open('tmp.json', 'w') as fp:
    dump({
            "runtimeOptions": {
                "tfm": "net5.0",
                "framework": {
                    "name": "Microsoft.NETCore.App",
                    "version": "5.0.0"
                 }
             }
         },
        fp, ensure_ascii=True, indent=4)
set_runtime(get_coreclr('tmp.json'))
remove('tmp.json')

from AlgorithmImports import *
from QuantConnect.Lean.Engine.DataFeeds import *
AddReference("Fasterflect")