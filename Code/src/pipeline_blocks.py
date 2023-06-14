# core libraries
import pickle
import sys
import pandas as pd 
import os 
import numpy as np
import json

import warnings
warnings.filterwarnings("ignore")

from sklearn.linear_model import LogisticRegression

# Import Packages 
import sys
import pandas as pd 
import os 
import json

src_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/code/src'
data_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/data/'
modules_dir = src_dir + '/modules'
params_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/code/params/'



# Opening Params Json File
params_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/code/params/'
f = open(params_dir + 'params.txt')
params_data = json.load(f)
print(f"params file loaded ..")

# import modules
src_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/code/src'
sys.path.insert(0, src_dir)
from utils import Utils

from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler, Normalizer


# import modules
modules_dir = '/Users/shashankgupta/Documents/code/git_project/redec/re_decisioning/code/src/modules'
sys.path.insert(0, modules_dir)
from preprocess import Convert, MissingValues, Outlier, FeatureSelection
from transform import Scaler, Transform, Selection
from modeling import ModelBuild, ModelMetric

# object initiation 
sc_min_max = Scaler(MinMaxScaler())
sc_std_scaler = Scaler(StandardScaler())
sc_robust_scaler = Scaler(RobustScaler())
sc_norm = Scaler(Normalizer())

tf = Transform()
sel = Selection()
ft = FeatureSelection()
cv = Convert()
mv = MissingValues()
ot = Outlier()
ut = Utils()

mb = ModelBuild()
mm = ModelMetric()


# Pipeline Class
class PipelineBlocks : 

    def __init__(self) -> None:
        pass

    def split_block(self, df, dv, test_size, seed):
        temp = df.copy()
        x_train, y_train, x_test, y_test = mb.split_test_train(temp, target_column = dv, test_size = test_size, random_state = seed)

        print(f'{x_train.shape = }', '|' ,f'{y_train.shape = }', '|' ,f'{x_test.shape = }', '|' ,f'{y_test.shape = }')
        return x_train, y_train, x_test, y_test

    def preprocess_block_train(self, x, thresh):
        temp = x.copy()

        print('# remove constant features')
        const_list = ut.get_const_features(temp)
        temp = temp.drop(columns=const_list,axis=1)
        print(temp.shape)
        
        print('# remove quasi constant features')
        qconst_list = ut.get_quasi_const_features(temp, threshold=0.01)
        temp = temp.drop(columns=qconst_list,axis=1)
        print(temp.shape)

        print('# imputing columns with respective median')
        temp = ut.fillnawith(temp,temp.columns,'median')
        
        bool_col_list = ut.findbool(temp)

        type_dct, type_dct_info = ut.get_datatypes_freq(temp)
        col_list = type_dct['float64']

        col_list_excpt_bool = [column for column in col_list if column not in bool_col_list]

        zero_list = ut.zero_value(temp,col_list_excpt_bool)

        col_list_excpt_bool = [column for column in col_list_excpt_bool if column not in zero_list]
    
        train_data_params = ut.create_data_params(temp, col_list_excpt_bool)

        train_min_max_params = ut.create_min_max_params(temp, col_list_excpt_bool)

        return temp, train_data_params, train_min_max_params, col_list_excpt_bool

    def preprocess_block_test(self, x_test, col_list_excpt_bool, train_data_params, train_min_max_params, scaler_object):
        temp = x_test.copy()

        # treat missing values
        for col in col_list_excpt_bool:
            temp[col].fillna(train_data_params[train_data_params['feature'] == col]['median'].values[0], inplace=True)

        # min max capping
        for col in col_list_excpt_bool:
            lower_cap = train_data_params[train_data_params['feature'] == col]['lower_limit'].values[0]
            upper_cap = train_data_params[train_data_params['feature'] == col]['upper_limit'].values[0]
            temp[col][temp[col] < lower_cap] = lower_cap
            temp[col][temp[col] > upper_cap] = upper_cap

        # scaling
        # temp.reset_index(drop=True,inplace=True)
        temp = pd.DataFrame(scaler_object.transform(temp[col_list_excpt_bool]), columns=col_list_excpt_bool)

        return temp

    def preprocess_block_train_testpipe(self, x, sel_features):
        temp = x[sel_features].copy()
        temp = ut.fillnawith(temp,temp.columns,'median')

        train_data_params = ut.create_data_params(temp, sel_features)

        train_min_max_params = ut.create_min_max_params(temp, sel_features)

        return temp, train_data_params, train_min_max_params, sel_features
        

    def preprocess_block(self, x, thresh, data_split):
        temp = x.copy()
        print(temp.shape)
        
        print(f"running dtype split ..")
        obj_cols, numeric_cols =  ut.split_dtypes(temp)
        obj_temp = temp[obj_cols]
        
        print(f"object types: {len(obj_cols)}")
        print(f"numeric_cols types: {len(numeric_cols)}")
        
        numeric_temp = ut.fillnawith(temp,numeric_cols,'median')
        bool_col_list = ut.findbool(numeric_temp)
        bool_temp = numeric_temp[bool_col_list]

        numeric_temp = numeric_temp.drop(bool_col_list,axis=1)

        print(f"# outlier treatment")
        temp, minmaxdf = tf.get_min_max_capping(numeric_temp,numeric_temp.columns, thresh)
        print(temp.shape)

        temp = temp.join(obj_temp)
        temp = temp.join(bool_temp)
        print(temp.shape)

        obj_cols, numeric_cols =  ut.split_dtypes(temp)

        print(f"object types: {len(obj_cols)}")
        print(f"numeric_cols types: {len(numeric_cols)}")

        if data_split == 'train':
            print('# remove constant features')
            const_list = ut.get_const_features(temp[numeric_cols])
            temp = temp[numeric_cols].drop(columns=const_list,axis=1)
            print(temp.shape)
            
            print('# remove quasi constant features')
            qconst_list = ut.get_quasi_const_features(temp, threshold=0.01)
            temp = temp.drop(columns=qconst_list,axis=1)
            
            # bool_col_list = ut.findbool(temp)
            # bool_temp = temp[bool_col_list]

            # obj_cols, numeric_cols =  ut.split_dtypes(temp)
            # obj_temp = temp[obj_cols]

            # temp = temp.drop(bool_col_list,axis=1)
            # temp = temp.drop(obj_temp,axis=1)

            # zero_arr  = ut.zero_value(temp,temp.columns)
            # temp = temp.drop(zero_arr,axis=1)

            # temp = temp.join(bool_temp)
            # temp = temp.join(obj_temp)

            print(temp.shape)
            
            print('# remove low variance features')

        return temp

    def scale_block(self, x, scale_type, data_split,obj=None):
        temp = x.copy()

        sc = Scaler(scale_type)    

        print('# running pipeline scale for x train')

        if data_split == 'train':
            scaled_df, scaler =  sc.get_scaled_df_train(temp, temp.columns )
            return scaled_df, scaler
        else:
            scaled_df =  sc.get_scaled_df_test(temp, obj, temp.columns )
            return scaled_df

    def transform_block(self, x, transform_type, power = None):
        temp = x.copy()

        tf = Transform()

        # isolate boolean features from scaling 
        bool_arr = ut.findbool(temp)
        bool_data = temp[bool_arr]

        # Dropping boolean features from df 
        df_test_p1 = temp.drop(bool_arr,axis=1)
        cols = df_test_p1.columns

        if transform_type == 'log':
            temp_transformed = tf.get_log_transform(temp,cols)
            temp_transformed = ut.fillnawith(temp_transformed,temp_transformed.columns,'median')
        elif transform_type == 'power':
            temp_transformed = tf.get_power_transform(temp,cols,power)
        elif transform_type == 'root':
            temp_transformed = tf.get_root_transform(temp,cols,power)
        
        return temp_transformed

    def feature_selection_block(self, x, y, bins, dependent_var, params):
        temp = x.copy()
        temp_all = temp.join(y)
        t1, t2 = sel.iv_woe(temp_all, dependent_var, bins = params['bins'] , show_woe=False)
        feature_list = t1[ (t1['IV']<params['upper_iv']) & (t1['IV']>params['lower_iv']) ]['Variable']
        print(f"features remaining afte IV elimination : {len(feature_list)}")

        print(f"# remove correlated features")
        print(f"starting feature elimination using correlation method, threshold : {params['corr_thresh']}")
        feature_list = sel.corr_iter(temp,feature_list , thresh= params['corr_thresh'])
        feature_list = list(feature_list)
        print(f"features remaining after corr elimination : {len(feature_list)}")
        
        # remove features with high vif
        print(f"starting feature elimination using VIF method, threshold : {params['vif_thresh']}..")
        feature_list, vif_df = sel.vif_iter(temp, feature_list, threshold= params['vif_thresh'])

        if params['forward_move']==True:
            move_type = 'Forward Selection'
        else: 
            move_type = 'Backward Selection'

        print(f'Starting {move_type}..')

        feat_list = ft.move_feature_selection(temp[feature_list], y, model = params['model'],forward=params['forward_move'], num_features=params['num_features'])
        print(f'Features remaning after {move_type} : {len(feat_list)}')

        return feat_list

    def model_training_block(self, x, y, params, tag):
        temp = x.copy()
        logreg_model = mb.classification_models(temp, y, params["params_log_reg"], models=['log_reg'])
        return logreg_model  

    def model_metrics_block(self, x, y, model, params, tag):
        temp = x.copy()
        model_output_dict = mm.model_metrics(model.predict(temp), np.array(y), model.predict_proba(temp),tag)
        dict_final = ut.append_dicts(model_output_dict,{'cols':temp.columns})
        dict_final = ut.append_dicts(params,dict_final)
    
        return dict_final

