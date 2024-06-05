import os, sys
class Config:
    def __init__(self, name):
        self.name = name

    def getDagId(self, dagname,filePath):
        module_dir_name = os.path.basename(os.path.dirname(os.path.dirname(filePath)))
        dagId = module_dir_name+"_"+dagname
        return dagId
    
    def getDagName(self, filePath):
        dagName = filePath.split(os.sep)[-1].replace('.py','')
        return dagName
    
    def getConfigurationsPath(self,filePath):
        configurationsPath = os.path.dirname(os.path.dirname(filePath))+os.sep+"Configurations"
        return configurationsPath
    
    def getConfigVariablePath(self,configurationsPath):
        configVariablePath = configurationsPath + "/config_variables.json"
        return configVariablePath
    
    def getSourceTableList(self,configurationsPath,fileName):
        sourceTableListPath = configurationsPath + "/"+ fileName
        return sourceTableListPath
    
   