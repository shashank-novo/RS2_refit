import numpy as np
from sklearn.feature_selection import VarianceThreshold
from sklearn.feature_selection import VarianceThreshold
from itertools import compress
import pandas as pd

class Utils:

    def __init__(self) -> None:
        pass

    # remove common entry in 2 arrays 
    def remove_common(self,arr1,arr2):
        try:
            if len(arr1)>len(arr2):
                temp_h = arr1 
                temp_l = arr2
            else:
                temp_l = arr1
                temp_h = arr2 
            A = [i for i in arr1 if i not in arr2]
        except: print('some error')

        return(A)

    # append 2 dicts together
    def append_dicts(self,dict1,dict2):
        temp = dict1.copy()
        for q in dict2:
            temp[q] = dict2[q]
        
        return temp

    # get boolean columns
    def findbool(self,df):
        bool_arr = []
        for col in df.columns: 
            if (len(df[col].unique())<=2):
                bool_arr.append(col)
        return bool_arr

    # get constant features
    def get_const_features(self,df):
        const_list = []
        for col in df.columns: 
            if (len(df[col].unique())==1):
                const_list.append(col)
        return const_list

    # get quasi-constant features
    def get_quasi_const_features(self,df, threshold=0.01):
        qconst_list = []
        for col in df.columns: 
            if (df[col].var() <= threshold):
                qconst_list.append(col)
        return qconst_list


    # view missing values
    def missing_value(self,df):
        percent_missing = df.isnull().sum() * 100 / len(df)
        missing_val_df = pd.DataFrame({'percent_missing': percent_missing})
        missing_val_df.sort_values(by='percent_missing', ascending=True, inplace=True)
        return missing_val_df

    ## view cols with 75% values equal to 0
    def zero_value(self,df,cols):
        temp = df.copy()
        zero_arr = []
        for col in cols:
                test = temp[col].describe().reset_index()[col].iloc[6]
                if test == 0 :
                        zero_arr.append(col)

        return zero_arr

    # get datatypes frequency
    def get_datatypes_freq(self,df):
        type_dct = {str(k): list(v) for k, v in df.groupby(df.dtypes, axis=1)}
        type_dct_info = {k: len(v) for k, v in type_dct.items()}
        return type_dct, type_dct_info
    
    # create dataparams for xtest fill 
    def create_data_params(self,df,col_list_excpt_bool):
        data_params = pd.DataFrame(columns=['feature', 'median', 'lower_limit', 'upper_limit'])
        data_params['feature'] = col_list_excpt_bool
        data_params['median'] = df[col_list_excpt_bool].median().values
        data_params['lower_limit'] = df[col_list_excpt_bool].quantile([0.01, 0.99]).values[0]
        data_params['upper_limit'] = df[col_list_excpt_bool].quantile([0.01, 0.99]).values[1]

        return data_params

    def create_min_max_params(self,df,col_list_excpt_bool):
        df_info = pd.DataFrame(columns=['feature', 'lower_cap', 'upper_cap', 'lower_values_capped', 'upper_values_capped'])
        df_info['feature'] = col_list_excpt_bool
        for col in col_list_excpt_bool:
            percentiles = df[col].quantile([0.01, 0.99]).values
            df_info.loc[df_info['feature'] == col, 'lower_cap'] = percentiles[0]
            df_info.loc[df_info['feature'] == col, 'upper_cap'] = percentiles[1]
            df_info.loc[df_info['feature'] == col, 'lower_values_capped'] = df[col][df[col] < percentiles[0]].shape[0]/df.shape[0]
            df_info.loc[df_info['feature'] == col, 'upper_values_capped'] = df[col][df[col] > percentiles[1]].shape[0]/df.shape[0]
            df[col][df[col] < percentiles[0]] = percentiles[0]
            df[col][df[col] > percentiles[1]] = percentiles[1]
        
        return df_info


    def intersection(self, lst1, lst2):
        lst3 = [value for value in lst1 if value in lst2]
        return lst3

    def gettime(self):
        dt = datetime.today().strftime('%Y-%m-%d')
        return dt

    def findnull(self,df,cols):
        t = pd.DataFrame(df[cols].isnull().sum())
        t.columns = ['Sum']
        return(t[t['Sum']>0])

    def finddtype(self,df,type):
        return(df.select_dtypes(include=[type]).dtypes.index)
    

    def converttype(self,df,cols,to_type):
        test = df[cols]
        for col in test.columns:
            test[col] = test[col].astype(to_type)
        
        return(test)

    def fillnawith(self, df,cols,fill_type):
        test = df[cols]

        for q in test.columns:
            if fill_type == 'mean':
                test[q] = np.where(test[q].isnull(),test[q].mean(),df[q])
            elif (fill_type == 'median'):

                test[q] = np.where(test[q].isnull(),test[q].median(),df[q])
            else : 
                test[q] = np.where(test[q].isnull(),test[q].mode(),df[q])
        
        return(test)

    def findoutlier(self, df,cols):
        test = df[cols]

        outlier_count = []
        iqr_arr = []
        q1_arr = []
        q3_arr = []
        lower_arr = []
        upper_arr = []

        for col in cols: 
            
            q1 = test[col].quantile(.25)
            q1_arr.append(q1)

            q3 = test[col].quantile(.75)
            q3_arr.append(q3)

            IQR = q3 - q1 
            iqr_arr.append(IQR)

            Ul = len(test[test[col]>(q3 + (1.5*IQR))])
            upper_arr.append(q3 + (1.5*IQR))

            Ll = len(test[test[col]<(q1 - (1.5*IQR))])
            lower_arr.append(q1 - (1.5*IQR))

            outlier_count.append(Ul+Ll)
            
        df_new = pd.DataFrame(columns=['Feature','Outlier_Count','IQR','Q1','Q3','LowerEnd','UpperEnd'])
        df_new['Feature'] = cols
        df_new['Outlier_Count'] = outlier_count
        df_new['IQR'] = iqr_arr
        df_new['Q1'] = q1_arr
        df_new['Q3'] = q3_arr
        df_new['LowerEnd'] = lower_arr
        df_new['UpperEnd'] = upper_arr

        return(df_new)

    def findbool(self,df,cols):
        test = df[cols]
        bool_arr = []
        for col in cols: 
            if (len(df[col].unique())<=2):
                bool_arr.append(col)
        
        return(bool_arr)
    
    def findseparation(self,df,cols,dv):
        test = df[cols]

        r1_arr = []
        r2_arr = []

        r2_by_r1_arr = []

        for col in cols: 
            ct = pd.crosstab(test[col],test[dv]).reset_index()
            r1 = ct[0].iloc[0]/ct[1].iloc[0]
            r1_arr.append(r1)

            r2 = ct[0].iloc[1]/ct[1].iloc[1]
            r2_arr.append(r2)

            r2_by_r1_arr.append(r2/r1)
        
        df_new = pd.DataFrame(columns=['Feature','Ratio 1','Ratio 2','Ratio 2 by Ratio 1'])
        df_new['Feature'] = cols 
        df_new['Ratio 1'] = r1_arr
        df_new['Ratio 2'] = r2_arr
        df_new['Ratio 2 by Ratio 1'] = r2_by_r1_arr

        return(df_new)

    # get boolean columns
    def findbool(self,df):
        bool_arr = []
        for col in df.columns: 
            if (len(df[col].unique())<=2):
                bool_arr.append(col)
        return(bool_arr)

    
    def make_node_connect(self,df,x,y):
        G = nx.from_pandas_edgelist(df, x, y)
        leaderboard = {}
        for x in G.nodes:
            leaderboard[x] = len(G[x])
        temp = pd.DataFrame(leaderboard.items(), columns=['Node', 'Connections']).sort_values('Connections', ascending=False)
        return(temp)

        # get feature list after iterative VIF elimination
    def vif_iter(df, iv, threshold=10):
        vif_data = pd.DataFrame()
        vif_data["feature"] = iv
        vif_data["VIF"] = [variance_inflation_factor(df[iv].values, i) for i in range(len(iv))]
        if len(vif_data[vif_data['VIF'] == np.inf]) > 0:
            feature = vif_data[vif_data['VIF'] == np.inf]['feature'].iloc[0]
            iv.remove(feature)
            vif_iter(df, iv, threshold)
        elif len(vif_data[vif_data['VIF'] >= threshold]) > 0:
            feature = vif_data.sort_values(by='VIF', ascending=False)['feature'].iloc[0]
            iv.remove(feature)
            vif_iter(df, iv, threshold)
        vif_data = pd.DataFrame()
        vif_data["feature"] = iv
        vif_data["VIF"] = [variance_inflation_factor(df[iv].values, i) for i in range(len(iv))]
        return iv, vif_data

    def split_dtypes(self, df):
            temp = df.copy()

            obj_cols = self.finddtype(temp,'object')
            
            float_cols = self.finddtype(temp,'float64')
            
            int_cols = self.finddtype(temp,'int64')

            numeric_cols = np.append(float_cols,int_cols)

            return obj_cols, numeric_cols
            
    

    def fs_variance(self, df, threshold:float=0.1):
        """
        Return a list of selected variables based on the threshold.
        """

        # The list of columns in the data frame
        features = list(df.columns)
        
        # Initialize and fit the method
        vt = VarianceThreshold(threshold = threshold)
        _ = vt.fit(df)
        
        # Get which column names which pass the threshold
        feat_select = list(compress(features, vt.get_support()))
        
        return feat_select