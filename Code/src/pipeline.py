# Import Packages 
import sys
import pandas as pd 
import os 
import json

src_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/code/src'
data_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/data/'
modules_dir = src_dir + '/modules'
params_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/code/params/'

# Reading Raw Data 
# df = pd.read_pickle(data_dir + 'novo_features_prescreen_2022-12-09.pkl')
# print(f"raw data file loaded with shape : {df.shape} ")

# Import Sklearn Modules 
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler, Normalizer
from sklearn.linear_model import LogisticRegression
import numpy as np

# Opening Params Json File
f = open(params_dir + 'params.txt')
params_data = json.load(f)
print(f"params file loaded ..")
params_data['model'] = LogisticRegression()
params_data['forward_move'] = True
params_data['params_log_reg']['class_weight'] = {0:0.1,1:0.9}
params_data['pipeline_os']['scale_type']= StandardScaler()
print(f"{params_data}")

# Import Utils Module
sys.path.insert(0,src_dir )
from utils import Utils
print(f"utils module loaded ..")

from pipeline_blocks import PipelineBlocks

# Import Other Modules
sys.path.insert(0, modules_dir)
from preprocess import Convert, MissingValues, Outlier, FeatureSelection
from transform import Scaler, Transform, Selection
from modeling import ModelBuild, ModelMetric
print(f"custom novo ds modules loaded ..")

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

print(f"all objects inititated ..")



print("""
Basic Flow : 

    Preprocess -> Transform -> Model Building -> Metrics 

""")


class PipelineTypes:

    def __init__(self) -> None:
        pass
        
    def Pipeline_OS(self, df, params):
        # Pipeline A : MinMax Capping + Scaling

        # 0. Split Block to create X Train, X Test, y train, y test
        x_train, y_train, x_test, y_test = pb.split_block(df, params['target'], params['test_size'], params['seed'])

        # 1. Preprocess Block for X Train
        temp_train = pb.preprocess_block(x_train, thresh = 0.3, data_split = 'train')
        print("preprocess step complete for x train")

        # 2. Scale Block for X Train
        temp_train, scale_train_object = pb.scale_block(temp_train, params['pipeline_os']['scale_type'],'train')
        print("scaling step complete for x train")

        # 3. Feature Selection Block for X Train
        selected_features = pb.feature_selection_block(temp_train, y_train, params['bins'], params['pipeline_os']['target'], params )

        # 4. Model Train Block for X Train
        train_model = pb.model_training_block(temp_train[selected_features], y_train, params= params,tag = "train")

        # 5. Model Metrics Block for X Train
        train_metrics = pb.model_metrics_block(temp_train[selected_features], y_train, train_model, params= params,tag = "train" )
        print("model metric collection complete for x train")

        # 6. Preprocess Block for X Test
        temp_test = pb.preprocess_block(x_test, thresh = 0.3, data_split = 'test')
        print("preprocess step complete for x test")

        # 7. Scale Block for X Test
        temp_test = pb.scale_block(temp_test[scale_train_object.feature_names_in_], params['pipeline_os']['scale_type'],"test",scale_train_object)
        print("scaling step complete for x test")

        # 8. Model Metrics Block for X Test
        test_metrics = pb.model_metrics_block(temp_test[selected_features], y_test, train_model, params= params,tag = "test" )
        print("model metric collection complete for x test")

        return train_model, train_metrics, test_metrics, 

    def Pipeline_OS_2(self, df, params):
        # Pipeline A : MinMax Capping + Scaling

        # 0. Split Block to create X Train, X Test, y train, y test
        x_train, y_train, x_test, y_test = pb.split_block(df, params['target'], params['test_size'], params['seed'])

        # 1. Preprocess Block for X Train
        temp_train, train_data_params, train_min_max_params, col_list_excpt_bool = pb.preprocess_block_train(x_train, thresh = 0.3)
        print("preprocess step complete for x train")

        # 2. Scale Block for X Train
        temp_train, scale_train_object = pb.scale_block(temp_train[col_list_excpt_bool], params['pipeline_os']['scale_type'],'train')
        print("scaling step complete for x train")

        # 3. Feature Selection Block for X Train
        selected_features = pb.feature_selection_block(temp_train, y_train, params['bins'], params['pipeline_os']['target'], params )
        # print(selected_features)

        # 4. Model Train Block for X Train
        train_model = pb.model_training_block(temp_train[selected_features], y_train, params= params,tag = "train")

        # 5. Model Metrics Block for X Train
        train_metrics = pb.model_metrics_block(temp_train[selected_features], y_train, train_model, params= params,tag = "train" )
        print("model metric collection complete for x train")

        # 6. Preprocess Block for X Test
        temp_test = pb.preprocess_block_test(x_test, col_list_excpt_bool, train_data_params, train_min_max_params, scale_train_object )
        temp_test.index = y_test.index
        print("preprocess step complete for x test")


        # 8. Model Metrics Block for X Test
        test_metrics = pb.model_metrics_block(temp_test[selected_features], y_test, train_model, params= params,tag = "test" )
        print("model metric collection complete for x test")

        test_ro, train_ro, all_ro = mm.rank_ordering_log_reg(train_model, temp_train, y_train, temp_test, y_test, train_metrics['cols'])

        return train_model, train_metrics, test_metrics, train_ro, test_ro, all_ro

    def Pipeline_O(self, df, params):
        # Pipeline A : MinMax Capping 

        # 0. Split Block to create X Train, X Test, y train, y test
        x_train, y_train, x_test, y_test = pb.split_block(df, params['target'], params['test_size'], params['seed'])

        # 1. Preprocess Block for X Train
        temp_train = pb.preprocess_block(x_train, thresh = 0.3, data_split = 'train')
        print("preprocess step complete for x train")

        # 3. Feature Selection Block for X Train
        selected_features = pb.feature_selection_block(temp_train, y_train, params['bins'], params['pipeline_os']['target'], params )

        # 4. Model Train Block for X Train
        train_model = pb.model_training_block(temp_train[selected_features], y_train, params= params,tag = "train")

        # 5. Model Metrics Block for X Train
        train_metrics = pb.model_metrics_block(temp_train[selected_features], y_train, train_model, params= params,tag = "train" )
        print("model metric collection complete for x train")

        # 6. Preprocess Block for X Test
        temp_test = pb.preprocess_block(x_test, thresh = 0.3, data_split = 'test')
        print("preprocess step complete for x test")

        # 8. Model Metrics Block for X Test
        test_metrics = pb.model_metrics_block(temp_test[selected_features], y_test, train_model, params= params,tag = "test" )
        print("model metric collection complete for x test")

        return train_model, train_metrics, test_metrics,

    def Pipeline_OLS(self, df, params  ):
        # Pipeline B : MinMax + Log + Scaling 

        # 0. Split Block to create X Train, X Test, y train, y test
        x_train, y_train, x_test, y_test = pb.split_block(df, params['target'], params['test_size'], params['seed'])

        temp_train = pb.preprocess_block(x_train, 0.3, data_split = 'train')
        print("preprocess step complete for x train")

        temp_train = pb.transform_block(temp_train, 'log')
        print("power transformation complete")

        temp_train, scale_train_object = pb.scale_block(temp_train, params['pipeline_os']['scale_type'],'train')
        print("scaling step complete for x train")

        selected_features = pb.feature_selection_block(temp_train, y_train, params['bins'], 'DPD_plus_15', params )

        train_model = pb.model_training_block(temp_train[selected_features], y_train, params= params,tag = "train")
        
        train_metrics = pb.model_metrics_block(temp_train[selected_features], y_train, train_model, params= params,tag = "train" )
        print("model metric collection complete for x train")

        temp_test = pb.preprocess_block(x_test, 0.3, data_split = 'test')
        print("preprocess step complete for x test")

        temp_test = pb.transform_block(temp_test, 'log')
        print("power transformation complete")

        temp_test = pb.scale_block(temp_test[scale_train_object.feature_names_in_], params['pipeline_os']['scale_type'],'test',scale_train_object)
        print("scaling step complete for x test")

        test_metrics = pb.model_metrics_block(temp_test[selected_features], y_test, train_model, params= params,tag = "test" )
        print("model metric collection complete for x test")

        return train_model, train_metrics, test_metrics,

class PipelineTest:

    def __init__(self) -> None:
        pass

    def Pipeline_OS_Test(self, df, selected_features, params):
        # Pipeline A : MinMax Capping + Scaling

        # 0. Split Block to create X Train, X Test, y train, y test
        x_train, y_train, x_test, y_test = pb.split_block(df, params['target'], params['test_size'], params['seed'])

        # 1. Preprocess Block for X Train
        temp_train = pb.preprocess_block(x_train, thresh = 0.3, data_split = 'train')
        minmax_train = temp_train.copy()
        print("preprocess step complete for x train")

        # 2. Scale Block for X Train
        temp_train, scale_train_object = pb.scale_block(temp_train, params['pipeline_os']['scale_type'],'train')
        print("scaling step complete for x train")

        # 4. Model Train Block for X Train
        train_model = pb.model_training_block(temp_train[selected_features], y_train, params= params,tag = "train")

        # 5. Model Metrics Block for X Train
        train_metrics = pb.model_metrics_block(temp_train[selected_features], y_train, train_model, params= params,tag = "train" )
        print("model metric collection complete for x train")


        # 6. Preprocess Block for X Test
        temp_test = pb.preprocess_block(x_test, thresh = 0.3, data_split = 'test')
        print("preprocess step complete for x test")

        # 7. Scale Block for X Test
        temp_test = pb.scale_block(temp_test[scale_train_object.feature_names_in_], params['pipeline_os']['scale_type'], "test", scale_train_object)
        print("scaling step complete for x test")

        # 8. Model Metrics Block for X Test
        test_metrics = pb.model_metrics_block(temp_test[selected_features], y_test, train_model, params= params,tag = "test" )
        print("model metric collection complete for x test")

        # 7. Scale Block for df 

        df_prep_businessid = df['business_id']
        df_prep_target = df['DPD_plus_15']
        df_prep = pb.preprocess_block(df, thresh = 0.3, data_split = 'train')
        df_prep = pb.scale_block(df_prep[scale_train_object.feature_names_in_], params['pipeline_os']['scale_type'], "entire", scale_train_object)
        df_prep = df_prep.join(df_prep_businessid)
        df_prep = df_prep.join(df_prep_target)
        
        print("scaling step complete for x test")
        # 5. Feature Importance 
        feat_imp = mm.feature_importance(train_model, df_prep[train_metrics['cols']])

        # 6. Rank Ordering 
        test_ro, train_ro, all_ro = mm.rank_ordering_log_reg(train_model, temp_train, y_train, temp_test, y_test, train_metrics['cols'])

        return feat_imp, train_ro, test_ro, all_ro, train_metrics, test_metrics, train_model, scale_train_object, minmax_train, temp_train, temp_test, y_train, y_test, df_prep


    def Pipeline_O_Test(self, df, selected_features, params):
        # Pipeline A : MinMax Capping + Scaling

        # 0. Split Block to create X Train, X Test, y train, y test
        x_train, y_train, x_test, y_test = pb.split_block(df, params['target'], params['test_size'], params['seed'])

        # 1. Preprocess Block for X Train
        temp_train = pb.preprocess_block(x_train, thresh = 0.3, data_split = 'train')
        print("preprocess step complete for x train")

        # 4. Model Train Block for X Train
        train_model = pb.model_training_block(temp_train[selected_features], y_train, params= params,tag = "train")

        # 5. Model Metrics Block for X Train
        train_metrics = pb.model_metrics_block(temp_train[selected_features], y_train, train_model, params= params,tag = "train" )
        print("model metric collection complete for x train")

        # 5. Feature Importance 
        feat_imp = mm.feature_importance(train_model, temp_train[train_metrics['cols']])

        # 6. Preprocess Block for X Test
        temp_test = pb.preprocess_block(x_test, thresh = 0.3, data_split = 'test')
        print("preprocess step complete for x test")

        # 8. Model Metrics Block for X Test
        test_metrics = pb.model_metrics_block(temp_test[selected_features], y_test, train_model, params= params,tag = "test" )
        print("model metric collection complete for x test")

        # 6. Rank Ordering 
        test_ro, train_ro = mm.rank_ordering_log_reg(train_model, temp_train, y_train, temp_test, y_test, train_metrics['cols'])

        return feat_imp, train_ro, test_ro, train_metrics, test_metrics, train_model

    
    def Pipeline_OS_2_Test(self, df, selected_features, params):
        # Pipeline A : MinMax Capping + Scaling

        # 0. Split Block to create X Train, X Test, y train, y test
        x_train, y_train, x_test, y_test = pb.split_block(df[np.append(selected_features,['DPD_plus_15'])], params['target'], params['test_size'], params['seed'])

        # 1. Preprocess Block for X Train
        temp_train, train_data_params, train_min_max_params, col_list_excpt_bool = pb.preprocess_block_train_testpipe(x_train, selected_features)
        print("preprocess step complete for x train")
        
        minmax_train = temp_train.copy()
        print("preprocess step complete for x train")

        # 2. Scale Block for X Train
        temp_train, scale_train_object = pb.scale_block(temp_train[selected_features], params['pipeline_os']['scale_type'],'train')
        print("scaling step complete for x train")

        # 4. Model Train Block for X Train
        train_model = pb.model_training_block(temp_train[selected_features], y_train, params= params,tag = "train")

        # 5. Model Metrics Block for X Train
        train_metrics = pb.model_metrics_block(temp_train[selected_features], y_train, train_model, params= params,tag = "train" )
        print("model metric collection complete for x train")

        # 7. Preprocess Block for X Test
        temp_test = pb.preprocess_block_test(x_test, selected_features, train_data_params, train_min_max_params, scale_train_object )
        temp_test.index = y_test.index
        print("preprocess step complete for x test")

        # 8. Model Metrics Block for X Test
        test_metrics = pb.model_metrics_block(temp_test[selected_features], y_test, train_model, params= params,tag = "test" )
        print("model metric collection complete for x test")

        # train cv AUC scores
        cv_scores = mm.cross_validation(train_model, temp_train[selected_features], y_train, scoring='roc_auc', folds=5, seed=params['seed'])
        print('CV Scores for Train -',np.round(cv_scores, 2))
        print('Mean of CV AUC Scores Train -',np.round(np.mean(cv_scores),2))

        # test cv AUC scores
        cv_scores = mm.cross_validation(train_model, temp_test[selected_features], y_test, scoring='roc_auc', folds=5, seed=params['seed'])
        print('CV AUC Scores for Test -',np.round(cv_scores, 2))
        print('Mean of CV AUC Scores Test -',np.round(np.mean(cv_scores),2))

        # train cv accuracy scores
        acc_scores = mm.cross_validation(train_model, temp_train[selected_features], y_train, scoring='accuracy', folds=5, seed=params['seed'])
        print('CV Accuracy Scores for Train -',np.round(acc_scores, 2))
        print('Mean of CV Accuracy Scores Train -',np.round(np.mean(acc_scores),2))

        # test cv accuracy scores
        acc_scores = mm.cross_validation(train_model, temp_test[selected_features], y_test, scoring='accuracy', folds=5, seed=params['seed'])
        print('CV Accuracy Scores for Test -',np.round(acc_scores, 2))
        print('Mean of CV Accuracy Scores Test -',np.round(np.mean(acc_scores),2))

        # 7. Scale Block for df 
        # df_prep_businessid = df['business_id']
        df_prep_target = df['DPD_plus_15']
        df_prep = pb.preprocess_block_test(df, selected_features, train_data_params, train_min_max_params, scale_train_object )
        df_prep.index = df.index
        # df_prep = df_prep.join(df_prep_businessid)
        df_prep = df_prep.join(df_prep_target)
        
        
        
        print("scaling step complete for x test")
        # 5. Feature Importance 
        feat_imp = mm.feature_importance(train_model, temp_train[train_metrics['cols']])

        # 6. Rank Ordering 
        test_ro, train_ro, all_ro = mm.rank_ordering_log_reg(train_model, temp_train, y_train, temp_test, y_test, train_metrics['cols'])

        return feat_imp, train_ro, test_ro, all_ro, train_metrics, test_metrics, train_model, scale_train_object, minmax_train, temp_train, temp_test, y_train, y_test,df_prep, train_data_params, train_min_max_params

