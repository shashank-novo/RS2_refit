from os import stat
import pandas as pd
import numpy as np
# from statsmodels.stats.outliers_influence import variance_inflation_factor
from scipy import stats

from mlxtend.feature_selection import SequentialFeatureSelector as sfs
from sklearn.linear_model import LogisticRegression

from fast_ml.utilities import display_all
from fast_ml.feature_selection import get_constant_features

import warnings

warnings.simplefilter("ignore")


class Convert:
    """Class to implement dtype conversion"""

    def __init__(self) -> None:
        """Initialize convert Class"""
    
    def str_to_numeric(self, df, cols):

        # df to be converted
        subset_df = df[cols]
        #Array to hold new col names
        numeric_cols = []

        for col in cols:
            col_name = f"numeric_{col}"
            subset_df[col_name] = subset_df[col].apply(pd.to_numeric)
            numeric_cols.append(col_name)

        return subset_df[numeric_cols]

    def str_to_datetime(self,df,cols,date_format):

        #df to be converted
        subset_df = df[cols]
        #Array to hold new col names
        date_cols = []

        for col in cols:
            col_name = f"datetime_{col}"
            subset_df[col_name] = pd.to_datetime(subset_df[col],format = date_format)
            date_cols.append(col_name)
        
        return subset_df[date_cols]


class MissingValues:
    """Class to impute missing data"""

    def __init__(self) -> None:
        """Initialize missing_values Class"""

    def fill_na(self,df,cols,value=None,method=None):

        #Value to use to fill holes (e.g. 0), alternately a dict/Series/DataFrame
        #Method to use for filling holes in reindexed Series pad / ffill: propagate last valid observation forward to next valid 
        #backfill / bfill: use next valid observation to fill gap.

        # Df to be imputed
        subset_df = df[cols]
        # Array to hold new column names
        miss_cols = []

        for col in cols:
            col_name = f"imputed_{col}"
            subset_df[col_name] = subset_df[[col]].fillna(value = value,method=method)
            miss_cols.append(col_name)

        return subset_df[miss_cols]

    
    def inter_polate(self, df, cols, method='linear',limit_direction=None,order=None):

        #method = 'linear', 'pad', 'polynomial'
        #limit_direction = 'forward','backward','both'
        #order = constant value, used when method = 'polynomial' else it will be None

        # Df to be imputed
        subset_df = df[cols]
        # Array to hold new column names
        miss_cols = []

        for col in cols:
            col_name = f"imputed_{col}"
            subset_df[col_name] = subset_df[col].interpolate(method=method,limit_direction=limit_direction,order=order)
            miss_cols.append(col_name)

        return subset_df[miss_cols]


#Outlier treatment

class Outlier:
    """Class to implement outlier treatment"""

    def __init__(self) -> None:
        """Initialize outlier Class"""

    def iqr_winsorize(self, df, cols, thresh=1.5):

        """
        Args:
            df (pd.DataFrame): dataframe that will be treated
            cols (np.ndarray): specific columns to be treated
            thresh (float): IQR Threshold around the mean which should be clipped
        Returns:
            1. df (pd.DataFrame): New Clipped Data
            2. df (pd.DataFrame): Feature wise how many rows were clipped on the lower and upper side
        """

        # Df to be imputed
        subset_df = df[cols]
        # Array to hold new column names
        clip_cols = []
        col_U_cnt = []
        col_L_cnt = []

        for col in cols:
            Q1 = subset_df[col].quantile(0.25)
            Q3 = subset_df[col].quantile(0.75)
            IQR = Q3 - Q1 
            
            lower_lim = Q1 - IQR*thresh
            upper_lim = Q3 + IQR*thresh 

            cnt_Upper = len(subset_df[subset_df[col] >= upper_lim])
            cnt_Lower = len(subset_df[subset_df[col] <= lower_lim])

            col_U_cnt.append(cnt_Upper)
            col_L_cnt.append(cnt_Lower)

            col_name = f"IQR_treatment_{col}"
            subset_df[col_name] = np.clip(subset_df[col], lower_lim, upper_lim)
            clip_cols.append(col_name)

        newDF = pd.DataFrame(
            columns=["column", "rows_clipped_lower", "rows_clipped_upper"]
        )
        newDF["column"] = cols
        newDF["rows_clipped_lower"] = col_L_cnt
        newDF["rows_clipped_upper"] = col_U_cnt

        return (subset_df[clip_cols], newDF)


class FeatureSelection:

    """Class to implement feature selection"""

    def __init__(self) -> None:
        """Initialize outlier Class"""

    def move_feature_selection(self, X, y, model, num_features=50, forward=True, scoring = 'roc_auc'):

        """
        Implements backward feature selection on-
        Args -->
        X - training data (no missing values)
        model - LogisticRegression()
        y - target variable
        num_features - length of final feature list
        scoring - evaluation metric

        Output --> list of size = 'num_features' containing top features names
        """
        
        for q in X.isnull().sum():
            if q != 0:
                raise ValueError("Some Column contains null or inf")

        model = model
        sfs1 = sfs(model, k_features=num_features, forward=forward, verbose=0, scoring=scoring)

        sfs1 = sfs1.fit(X, y)

        feat_names = list(sfs1.k_feature_names_)

        return feat_names


    def constant_features(self, df):
        """  
        returns a list of constant features
        """

        constant_features = get_constant_features(df)
        constant_features_list = constant_features.query("Desc=='Constant")['Var'].to_list()

        return constant_features_list


    def quasi_constant_features(self, df, thresh):
        """  
        returns a list of quasi-constant features
        """

        constant_features = get_constant_features(df, threshold = thresh, dropna = False)
        quasi_constant_features_list = constant_features.query("Desc=='Quasi Constant")['Var'].to_list()

        return quasi_constant_features_list


