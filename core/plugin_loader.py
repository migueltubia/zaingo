import logging
import os
import inspect
from importlib import import_module
from plugins import plugin_interface
from plugins import InputPlugin
from plugins import OutputPlugin
from plugins import analysis_interface


PLUGINS_FOLDER="/plugins"
MAIN_FILE="main.py"


def load_plugins():
    """This function loads all plugins from plugins folder. It returns a list, with three subdicts and keys ["input", "outputç" and "rngine"]] key=engine.name and value=engine class itself"""
    logger = logging.getLogger('plugin_loader')
    logger.debug("Init loading plugins")
    classList={}
    classList['inputs'] = {}
    classList['outputs'] = {}
    classList['engines'] = {}

    path=os.getcwd()+"/"+PLUGINS_FOLDER

    candidates = [f.name for f in os.scandir(path) if f.is_dir() and f.name!="__pycache__"]
    if candidates:
        for c in candidates:
            modname = "plugins."+c+".main"
            logger.debug("%s: Importing module", modname)
            try:
                module=import_module(modname)
            except NotImplementedError as e:
                logger.error("NotImplementedError: Error loading module: %s", e)
                continue
            except ImportError as e:
                logger.error("ImportError: Error loading module %s: %s", c, e)
                continue
            clases=None
            try:
                clases = [c[0] for c in inspect.getmembers(module, inspect.isclass) if issubclass(getattr(module, c[0]), plugin_interface.PluginInterface)]
            except Exception as e:
                logger.error("Load error: Error loading module %s with basename %s: %s",module, e)
                continue
            if clases!=None:
                for c in clases:
                    cls=getattr(module, c)
                    #: Here we have to ensure that the class is a subclass of analysis.analysis_interface.EngineInterface
                    if (issubclass(cls, InputPlugin.InputPlugin)):
                        logger.debug("%s: Loading plugin", modname)
                        _object = cls()
                        classList["inputs"][_object.name]=_object
                    elif (issubclass(cls, OutputPlugin.OutputPlugin)):
                        logger.debug("%s: Loading plugin", modname)
                        _object = cls()
                        classList["outputs"][_object.name]=modname
                    elif (issubclass(cls,analysis_interface.EngineInterface)):
                        logger.debug("%s: Loading plugin", modname)
                        _object = cls()
                        classList["engines"][_object.name]=_object
        logger.debug("End of register plugins")
    return classList
