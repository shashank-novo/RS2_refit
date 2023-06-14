from os import stat
from xml.etree.ElementTree import QName
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, Normalizer
from statsmodels.stats.outliers_influence import variance_inflation_factor
from scipy import stats

import warnings

warnings.simplefilter("ignore")

import networkx as nx



class Scaler:
    """Scaler Class to implement StandardScaler, MinMaxScaler, RobustScaler, Normalizer Scaling"""

    def __init__(self, scale_type) -> None:
        """Initialize Scaler Class

        Args:
            scale_type (sklearn.preprocessing): Use StandardScaler(), MinMaxScaler(), RobustScaler(), Normalizer()
        """
        self.scale_type = scale_type

    def get_scaled_df_test(self, xtest, obj, cols):
        """Performs and Returns a Scaled version of the input columns according to Scale Type passed

        Args:
            xtest (pd.DataFrame): dataframe that will be scaled
            cols (np.ndarray): columns to be scaled

        Returns:
            Original Columns + Scaled Columns
        """
        # Prefix to be chosen according to Scale Type
        scaletype_alias_dict = {
            "StandardScaler()": "standardscaler",
            "MinMaxScaler()": "minmaxscaler",
            "RobustScaler()": "robustscaler",
            "Normalizer()": "normalizer",
        }

        # Scale Type Chosen
        chosen_scale = str(self.scale_type)
        # Df to be scaled
        subset = xtest[cols]
        
        # Array to hold new column names
        scaled_features_cols = []

        # Fit Transform to get the scaled columns
        scaled_features_df = obj.transform(subset)

        # Appending the new column names into scaled_features_cols
        for col in subset.columns:
            new_col = f"{scaletype_alias_dict[chosen_scale]}_{col}"
            scaled_features_cols.append(new_col)

        # Converting the scaled array into a dataframe
        scaled_df = pd.DataFrame(
            scaled_features_df, index=subset.index, columns=scaled_features_cols
        )

        return scaled_df

    def get_scaled_df_train(self, xtrain, cols):
        """Performs and Returns a Scaled version of the input columns according to Scale Type passed

        Args:
            df (pd.DataFrame): dataframe that will be scaled
            cols (np.ndarray): columns to be scaled

        Returns:
            Original Columns + Scaled Columns
        """
        # Prefix to be chosen according to Scale Type
        scaletype_alias_dict = {
            "StandardScaler()": "standardscaler",
            "MinMaxScaler()": "minmaxscaler",
            "RobustScaler()": "robustscaler",
            "Normalizer()": "normalizer",
        }

        # Scale Type Chosen

        scaler = self.scale_type

        chosen_scale = str(scaler)
        # Df to be scaled
        subset = xtrain[cols]

        # Array to hold new column names
        scaled_features_cols = []

        # Fit Transform to get the scaled columns
        scaled_features_df = scaler.fit_transform(subset)

        # Appending the new column names into scaled_features_cols
        for col in subset.columns:
            new_col = f"{col}"
            scaled_features_cols.append(new_col)

        # Converting the scaled array into a dataframe
        scaled_df = pd.DataFrame(
            scaled_features_df, index=subset.index, columns=scaled_features_cols
        )

        return scaled_df,scaler    


class Transform:
    """Transform Class to implement Log, Power, SquareRoot Transformations"""

    def __init__(self) -> None:
        """Initialize Transform Class"""

    def get_log_transform(self, df, cols):
        """Returns Log Transformations for the columns mentioned for the dataframe

        Args:
            df (pd.DataFrame): dataframe that will be log transformed
            cols (np.ndarray): specific columns to be log transformed
        """
        # Df to be transformed
        subset_df = df[cols]
        # Array to hold new column names
        log_cols = []

        for col in cols:
            col_name = f"log_{col}"
            subset_df[col_name] = subset_df[col]+1
            subset_df[col_name] = np.log1p(subset_df[col_name])
            log_cols.append(col_name)

        return subset_df[log_cols]

    def get_power_transform(self, df, cols, power):
        """Returns Power Transformations for the columns mentioned for the dataframe

        Args:
            df (pd.DataFrame): dataframe that will be power transformed
            cols (np.ndarray): specific columns to be power transformed
        """
        subset_df = df[cols]
        # Array to hold new column names
        power_cols = []
        for col in cols:
            col_name = f"power_{power}_{col}"
            subset_df[col_name] = np.power(subset_df[col], power)
            power_cols.append(col_name)

        return subset_df[power_cols]

    def get_min_max_capping(self, df, cols, thresh):

        """Clip Data according to n*standard_deviation around mean

        Args:
            df (pd.DataFrame): dataframe that will be min max capped
            cols (np.ndarray): specific columns to be min max capped
            thresh (float): Threshold around the mean which should be clipped

        Returns:
            1. df (pd.DataFrame): New Clipped Data
            2. df (pd.DataFrame): Feature wise how many rows were clipped on the lower and upper side

        """
        subset_df = df[cols]
        clip_cols = []
        col_U_cnt = []
        col_L_cnt = []

        for col in cols:
            avg = np.mean(subset_df[col])
            sd = np.std(subset_df[col])

            U = avg + (thresh * sd)
            L = avg - (thresh * sd)

            cnt_Upper = len(subset_df[subset_df[col] >= U])
            cnt_Lower = len(subset_df[subset_df[col] <= L])

            col_U_cnt.append(cnt_Upper)
            col_L_cnt.append(cnt_Lower)

            col_name = f"min_max_transform_{col}"
            subset_df[col_name] = np.clip(subset_df[col], L, U)
            clip_cols.append(col_name)

        newDF = pd.DataFrame(
            columns=["column", "rows_clipped_lower", "rows_clipped_upper"]
        )
        newDF["column"] = cols
        newDF["rows_clipped_lower"] = col_L_cnt
        newDF["rows_clipped_upper"] = col_U_cnt

        return (subset_df[clip_cols], newDF)

    def get_root_transform(self, df, cols, root):
        """Perform Root Transformations for the columns mentioned for the dataframe

        Args:
            df (pd.DataFrame): dataframe that will be root transformed
            cols (np.ndarray): _description_

        Returns:
            df (pd.DataFrame): Root Transformed data
        """
        subset_df = df[cols]
        # Array to hold new column names
        root_cols = []
        for col in cols:
            col_name = f"root_{root}_{col}"
            subset_df[col_name] = np.power(subset_df[col], root)
            root_cols.append(col_name)

        return subset_df[root_cols]

class Utils : 
    def __init__(self) -> None:
        """Initialize Utils Class"""

    def make_node_connect(self,df,x,y):
        G = nx.from_pandas_edgelist(df, x, y)
        leaderboard = {}
        for x in G.nodes:
            leaderboard[x] = len(G[x])
        temp = pd.DataFrame(leaderboard.items(), columns=['Node', 'Connections']).sort_values('Connections', ascending=False)
        return(temp)

ut = Utils()

class Selection:
    """Selection Class to calculate correlations and VIF"""

    def __init__(self) -> None:
        """Initialize Selection Class"""

    def get_correlated_features(self, df, IV, thresh):
        """Returns  Features List where correlation is above given threshold

        Args:
            df (pd.DataFrame): dataframe whose correlation needs to be calculated
            IV (np.ndarray): _description_
        """
        subset_df = df[IV]
        subset_df_corr = subset_df.corr()

        thres = thresh
        feature1, feature2, threshold_arr = [], [], []

        for q in subset_df_corr.columns:
            for r in subset_df_corr.index:
                if (abs(subset_df_corr[q][r]) > thres) & (subset_df_corr[q][r] != 1):
                    feature1.append(q)
                    feature2.append(r)
                    threshold_arr.append(subset_df_corr[q][r])

        above_threshold_df = pd.DataFrame(columns=["feature_1", "feature_2", "corr"])
        above_threshold_df["feature_1"] = feature1
        above_threshold_df["feature_2"] = feature2
        above_threshold_df["corr"] = threshold_arr

        return (above_threshold_df, subset_df_corr)

    def get_VIF(self, df, IV, thresh):
        """Fetch the VIF of columns mentioned for the given DataFrame

        Args:
            df (pd.DataFrame): dataframe whose VIF needs to be calculated
            IV (np.ndarray): _description_
        """
        vif_data = pd.DataFrame()
        X = df[IV]

        for q in X.isnull().sum():
            if q != 0:
                raise ValueError("Some Column contains null or inf")

        vif_data["feature"] = X.columns
        vif_data["VIF"] = [
            variance_inflation_factor(X.values, i) for i in range(len(X.columns))
        ]

        return (vif_data, vif_data[vif_data["VIF"] >= thresh])

    def corr_iter(self,df,cols,thresh):
        """ Algorithm to drop features with the highest nodes among other highly correlated features (corr > Thresh)

        Args:
            df (_type_): Dataframe used
            cols (_type_): List of Features
            thresh (_type_): Above which cols need to be dropped
        """
        
        T1,T2 = self.get_correlated_features(df,cols,thresh)
        corr_df_len = len(T1)

        while (corr_df_len>0):
            t1,t2 = self.get_correlated_features(df,cols,thresh)
            corr_df_len = len(t1)
            try: 
                corr_nodes = ut.make_node_connect(t1,'feature_1','feature_2')
                exclude_Node = corr_nodes['Node'].iloc[0]
                cols = cols[cols!=exclude_Node]
            except: 
                return(cols)

    # get feature list after iterative VIF elimination
    def vif_iter(self, df, iv, threshold=10):
        vif_data = pd.DataFrame()
        vif_data["feature"] = iv
        vif_data["VIF"] = [variance_inflation_factor(df[iv].values, i) for i in range(len(iv))]
        if len(vif_data[vif_data['VIF'] == np.inf]) > 0:
            feature = vif_data[vif_data['VIF'] == np.inf]['feature'].iloc[0]
            iv.remove(feature)
            self.vif_iter(df, iv, threshold)
        elif len(vif_data[vif_data['VIF'] >= threshold]) > 0:
            feature = vif_data.sort_values(by='VIF', ascending=False)['feature'].iloc[0]
            iv.remove(feature)
            self.vif_iter(df, iv, threshold)
        vif_data = pd.DataFrame()
        vif_data["feature"] = iv
        vif_data["VIF"] = [variance_inflation_factor(df[iv].values, i) for i in range(len(iv))]
        return iv, vif_data

    def iv_woe(self,data, target, bins=10, show_woe=False):
    
        #Empty Dataframe
        newDF,woeDF = pd.DataFrame(), pd.DataFrame()
        
        #Extract Column Names
        cols = data.columns
        
        #Run WOE and IV on all the independent variables
        for ivars in cols[~cols.isin([target])]:
            if (data[ivars].dtype.kind in 'bifc') and (len(np.unique(data[ivars]))>10):
                binned_x = pd.qcut(data[ivars], bins,  duplicates='drop')
                d0 = pd.DataFrame({'x': binned_x, 'y': data[target]})
            else:
                d0 = pd.DataFrame({'x': data[ivars], 'y': data[target]})

            
            # Calculate the number of events in each group (bin)
            d = d0.groupby("x", as_index=False).agg({"y": ["count", "sum"]})
            d.columns = ['Cutoff', 'N', 'Events']
            
            # Calculate % of events in each group.
            d['% of Events'] = np.maximum(d['Events'], 0.5) / d['Events'].sum()

            # Calculate the non events in each group.
            d['Non-Events'] = d['N'] - d['Events']
            # Calculate % of non events in each group.
            d['% of Non-Events'] = np.maximum(d['Non-Events'], 0.5) / d['Non-Events'].sum()

            # Calculate WOE by taking natural log of division of % of non-events and % of events
            d['WoE'] = np.log(d['% of Events']/d['% of Non-Events'])
            d['IV'] = d['WoE'] * (d['% of Events'] - d['% of Non-Events'])
            d.insert(loc=0, column='Variable', value=ivars)
            #print("Information value of " + ivars + " is " + str(round(d['IV'].sum(),6)))
            #print(ivars)
            temp =pd.DataFrame({"Variable" : [ivars], "IV" : [d['IV'].sum()]}, columns = ["Variable", "IV"])
            newDF=pd.concat([newDF,temp], axis=0)
            woeDF=pd.concat([woeDF,d], axis=0)

            #Show WOE Table
            if show_woe == True:
                print(d)
                
        return newDF, woeDF


