# Import Packages 
import sys
import pandas as pd 
import os 
import json
import pickle 

# src_dir = '/Users/debrishidas/Documents/Work/Projects/tokyo/code/src'
# data_dir = '/Users/debrishidas/Documents/Work/Projects/tokyo/data/raw/'
# modules_dir = src_dir + '/modules'
# params_dir = '/Users/debrishidas/Documents/Work/Projects/tokyo/code/params/'

# Reading Raw Data 
# df = pd.read_pickle(data_dir + 'novo_features_prescreen_2022-10-31.pkl')
#print(f"raw data file loaded with shape : {df.shape} ")

# Import Sklearn Modules 
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler, Normalizer
from sklearn.linear_model import LogisticRegression
import numpy as np

# # Opening Params Json File
# f = open(params_dir + 'params.txt')
# params_data = json.load(f)
# print(f"params file loaded ..")
# params_data['model'] = LogisticRegression()
# params_data['forward_move'] = True
# params_data['params_log_reg']['class_weight'] = {0:0.2,1:0.8}
# params_data['pipeline_os']['scale_type']= StandardScaler()
# print(f"{params_data}")

# move to previous direc
# os.chdir('../')
# cwd = os.getcwd()
# print(cwd)

# # move to code/src direc
# os.chdir('./code/src')
# cwd = os.getcwd()
# print(cwd)

from utils import Utils
from pipeline_blocks import PipelineBlocks
from pipeline import PipelineTypes, PipelineTest

# move to modules direc
# os.chdir('./modules')
# cwd = os.getcwd()
# print(cwd)

# Import Other Modules
from preprocess import Convert, MissingValues, Outlier, FeatureSelection
from transform import Scaler, Transform, Selection
from modeling import ModelBuild, ModelMetric


print(f"custom novo ds modules loaded ..")


# Opening Params Json File

# move to previous direc
# os.chdir('../')
# cwd = os.getcwd()
# print(cwd)

# # move to previous direc
# os.chdir('../')
# cwd = os.getcwd()
# print(cwd)

# # move to params folder
# os.chdir('./params')
# cwd = os.getcwd()
# print(cwd)

# Object Initiation 
sc_min_max = Scaler(MinMaxScaler())
sc_std_scaler = Scaler(StandardScaler())
sc_robust_scaler = Scaler(RobustScaler())
sc_norm = Scaler(Normalizer())

cv = Convert()
mv = MissingValues()
ot = Outlier()
ut = Utils()

tf = Transform()
sel = Selection()
ft = FeatureSelection()

mb = ModelBuild()
mm = ModelMetric()

pb = PipelineBlocks()
pt = PipelineTypes()

print(f"all objects inititated ..")



print("""
Basic Flow : 

    Preprocess -> Transform -> Model Building -> Metrics 

""")
class Grid: 

    def __init__(self) -> None:
         pass

    def run_grid_search_pipe_OS(self,corr_arr,vif_arr,features_arr,fmove_arr,df):

        # Experiment df to hold pipeline results 
        experiment_columns = ['cols','corr_thresh','vif_thresh','forward_move','num_features','Train_AUC','Test_AUC','seed','is_train_ro','is_test_ro','is_all_ro']

        experiments_df = pd.DataFrame(columns = experiment_columns)
        model_arr = []

        for seed in range(1,25):
            for p in corr_arr:
                for q in vif_arr:
                    for r in features_arr: 
                        for s in fmove_arr:
                                params = {"seed" : seed,
                                        "test_size" : 0.3,
                                        "corr_thresh": p,
                                        "vif_thresh": q,
                                        "target" : "DPD_plus_15", 
                                        "model" : LogisticRegression(),
                                        "forward_move" : s,
                                        "num_features" : r,
                                        "bins" : 10,
                                        "upper_iv" : 0.5,
                                        "lower_iv" : 0.02,
                                        "params_log_reg" : {"penalty": "l1", "random_state": 42, "solver": "liblinear", "class_weight":{0:0.08,1:0.92}},
                                        "pipeline_os" : {"scale_type" : StandardScaler(), "target" : "DPD_plus_15"}}

                                print(f"Number of Models Done : {len(experiments_df)}")
                                
                                train_model, train_metrics, test_metrics, train_ro, test_ro, all_ro = pt.Pipeline_OS_2(df, params)

                                model_arr.append(train_model)

                                #pickle.dump('/Users/debrishidas/Documents/Work/novo_git/lending-rs1.1/models/varmodels/')
                                
                                
                                train_auc = train_metrics['AUC']
                                test_auc = test_metrics['AUC']
                                n_features = train_metrics['num_features']
                                corr_threshold = train_metrics['corr_thresh']
                                vif_threshold = train_metrics['vif_thresh']
                                forward_move = train_metrics['forward_move']
                                cols = np.array(train_metrics['cols'])
                                is_train_ro = train_ro['DPD_plus_15_rate (%)'].is_monotonic_increasing
                                is_test_ro = test_ro['DPD_plus_15_rate (%)'].is_monotonic_increasing
                                is_all_ro = all_ro['DPD_plus_15_rate (%)'].is_monotonic_increasing
                        

                                
                                row_dict = {'cols': cols,'corr_thresh' :corr_threshold ,'vif_thresh' : vif_threshold,\
                                    'forward_move':forward_move ,'num_features' : n_features,'Train_AUC' :train_auc, 'Test_AUC' : test_auc, 'seed':params['seed'],\
                                         'is_train_ro':is_train_ro, 'is_test_ro':is_test_ro, 'is_all_ro':is_all_ro}
                                
                                experiments_df = experiments_df.append(row_dict, ignore_index=True)
        return experiments_df, model_arr

    

    def run_grid_search_O(self, corr_arr,vif_arr,features_arr,fmove_arr,df):

        # Experiment df to hold pipeline results 
        experiment_columns = ['cols','corr_thresh','vif_thresh','forward_move','num_features','Train_AUC','Test_AUC']

        experiments_df = pd.DataFrame(columns = experiment_columns)
        experiments_df

            
        for p in corr_arr:
            for q in vif_arr:
                for r in features_arr: 
                    for s in fmove_arr:
                            params = {"seed" : 42,
                                    "test_size" : 0.3,
                                    "corr_thresh": p,
                                    "vif_thresh": q,
                                    "target" : "DPD_plus_15", 
                                    "model" : LogisticRegression(),
                                    "forward_move" : s,
                                    "num_features" : r,
                                    "bins" : 10,
                                    "upper_iv" : 0.5,
                                    "lower_iv" : 0.02,
                                    "params_log_reg" : {"penalty": "l1", "random_state": 42, "solver": "liblinear", "class_weight":{0:0.08,1:0.92}},
                                    "pipeline_os" : {"scale_type" : StandardScaler(), "target" : "DPD_plus_15"}}

                            print(f"Number of Models Done : {len(experiments_df)}")
                            
                            train_model, train_metrics, test_metrics = pt.Pipeline_O(df, params)
                            
                            train_auc = train_metrics['AUC']
                            test_auc = test_metrics['AUC']
                            n_features = train_metrics['num_features']
                            corr_threshold = train_metrics['corr_thresh']
                            vif_threshold = train_metrics['vif_thresh']
                            forward_move = train_metrics['forward_move']
                            cols = np.array(train_metrics['cols'])
                            
                            row_dict = {'cols': cols,'corr_thresh' :corr_threshold ,'vif_thresh' : vif_threshold,\
                                'forward_move':forward_move ,'num_features' : n_features,'Train_AUC' :train_auc, 'Test_AUC' : test_auc }
                            
                            experiments_df = experiments_df.append(row_dict, ignore_index=True)


        return experiments_df 

