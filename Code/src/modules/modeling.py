#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------
# Created By  : Sarfraz Ahmned
# Created Date: 13/10/2022
# version ='1.0'
# ---------------------------------------------------------------------------
""" Contains the modules for model building, feature importance and hyperparameter tuning"""
# ---------------------------------------------------------------------------

# core libraries
import pandas as pd
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, roc_curve, auc, f1_score
from scipy.stats import ks_2samp
from sklearn.model_selection import cross_val_score, KFold, GridSearchCV, train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression



# plotting libraries
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio
# import scikitplot as skplt

class ModelBuild:

    def __init__(self) -> None:
        pass
    
    def split_test_train(self, df, target_column, test_size, random_state):
            X_train, X_test, y_train, y_test = train_test_split(df.drop(target_column, axis=1),
                                                                df[target_column],
                                                                test_size=test_size,
                                                                stratify=df[target_column], 
                                                                random_state=random_state)
            X_train = X_train
            X_test = X_test
            y_train = y_train
            y_test = y_test

            return X_train, y_train, X_test, y_test


    # Hyperparameter tuning --------------------------------------------------------------------------------------------------------
    def tune_hyperparameters(self, X: np.ndarray, y: np.ndarray, model: object, parameters: dict, metric='accuracy', cv_folds=5):
        '''
        Returns the model object with tuned hyperparameters

            Parameters:
                    X (array(int)): multi-dimensional array of predictor variables
                    y (array(int)): array of actual labels consisting of only 0 and 1
                    model (object): model object of the binary classifier
                    parameters (dict): dict of hyperparameters to be used for searching
                    metric (str, optional): scoring method to be used, use sklearn.metrics.get_scorer_names() to see the complete list
                    cv_folds (int, optional): total splits of the data to fit the model

            Returns:
                    best_estimator (object): model object with tuned hyperparameters

            Raises:
                    ValueError: If length of X is not equal to length of y
        '''
        # Check if arrays are of different length
        if len(X) != len(y):
            raise ValueError('Length of predictor and dependent is not matching')

        # Initialize the grid search object with the required params
        grid_search = GridSearchCV(estimator=model, param_grid=parameters, scoring = metric, n_jobs = -1, cv = cv_folds, verbose=False)

        # Fit and search the optimal hyperparameters
        grid_search.fit(X, y)

        # Return the best fit model object
        return grid_search.best_estimator_


    # Feature Encoding --------------------------------------------------------------------------------------------------------
    def feature_encoding(self, data=pd.DataFrame(), encode_columns=[], encode_type='onehot', max_unique_values=20):
        """
        Function to encode the categorical variables
        'data' is necessary parameter and 'encode_columns' & 'encode_type' are optional parameters

            Parameters:
                data (dataframe): Dataframe dataset
                encode_columns (list): List of columns that require encoding
                encode_type (string): 'onehot' or 'label' encoding methiods

            Returns:
                data (dataframe): Transformed dataframe

            Raises:
                TypeError: If lenght of input data is zero
        """     
        if data.shape[0] > 0:

            # get columns from user or by data types
            if len(encode_columns) == 0:
                cat_columns = [col for col in data.columns if data[col].dtype.name in ['object','category','bool']]
            else:
                cat_columns = encode_columns

            # filter out columns that are unique identifiers
            cat_columns = [col for col in cat_columns if data[col].agg(['nunique'])[0] <= max_unique_values]
            rest_columns = list(set(data.columns)-set(cat_columns))

            # encode with one-hot encoding
            if encode_type == 'onehot':
                cat_data = pd.get_dummies(data[cat_columns])
                if len(rest_columns) > 0:
                    rest_data = data[rest_columns]
                    data = pd.concat([rest_data, cat_data], axis=1)
                else:
                    data = cat_data

            # encode with label encoding
            elif encode_type == 'label':
                data_tmp = pd.DataFrame(columns=cat_columns)
                for col in cat_columns:
                    data_tmp[col] = data[col].astype('category').cat.codes

                if len(rest_columns) > 0:
                    rest_data = data[rest_columns]
                    data = pd.concat([rest_data, data_tmp], axis=1)
                else:
                    data = data_tmp
            else:
                raise ValueError('Invalid encoding type')
        else:
            raise TypeError('No data input or input data has zero records')
        return data


    # Model Building --------------------------------------------------------------------------------------------------------
    def classification_models(self, x_train, y_train, params_log_reg={}, params_dtc={}, models=[]):
        """
        Function to train the linear, logistic, decision trees.
        'train_data' is necessary parameter and remaining are optional parameters
            Parameters:
                x_train (dataframe): Dataframe dataset
                y_train (dataframe): Dataframe dataset
                params_log_reg (dict): logistic regression parameters
                params_dtc (dict): decision tree parameters
                models (list): ['log_reg','svc','dtc','rfc','xgbc']
            Returns:
                log_reg (object): trained model output
                dtc (object): trained model output
        


        """
        if models == [] or 'log_reg' in models:
            if params_log_reg == {}:
                log_reg = LogisticRegression().fit(x_train, y_train)
            else:
                log_reg = LogisticRegression()
                log_reg.set_params(**params_log_reg)
                log_reg.fit(x_train, y_train)
            return log_reg
        if models == [] or 'dtc' in models:
            if params_dtc == {}:
                dtc = DecisionTreeClassifier().fit(x_train, y_train)
            else:
                dtc = DecisionTreeClassifier()
                dtc.set_params(**params_log_reg)
                dtc.fit(x_train, y_train)
            return dtc
    

# core libraries
import pandas as pd
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, roc_curve, auc, f1_score
from scipy.stats import ks_2samp
from sklearn.model_selection import cross_val_score, KFold, GridSearchCV

# plotting libraries
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio
import scikitplot as skplt

mb = ModelBuild()



class ModelMetric:

    def __init__(self) -> None:
        pass

    # Model Evalution Metrics --------------------------------------------------------------------------------------------------------
    def model_metrics(self, y_pred: np.ndarray, y_true: np.ndarray, y_prob: np.ndarray,tag):
        '''
        Returns the evaluation metrics for a binary classification model

            Parameters:
                    y_pred (array(int)): array of predicted labels consisting of only 0 and 1
                    y_true (array(int)): array of actual labels consisting of only 0 and 1
                    y_prob (array(int)): 2 dimensional array of predicted probabilities
            
            Prints:
                    accuracy score (int): accuracy score of the binary classifier
                    confusion matrix (int): confusion matrix of the binary classifier
                    fpr (int): false positive rate of the binary classifier
                    tpr (int): true positive rate of the binary classifier
                    auc (int): area under curve score for the binary classifier
                    sensitivity (int): sensitivity of the binary classifier
                    specificity (int): specificity of the binary classifier
                    f1-score (int): F1-score of the binary classifier
                    ks-score (int): KS score of the binary classifier
                    classification report (int): elaborate precision recall report of the binary classifier
                    gain chart (plot): gain chart plot of the binary classifier
                    lift chart (plot): lift chart plot of the binary classifier
                    
            Raises:
                    ValueError: If length of y_pred is not equal to length of y_true or X_test
        '''

        # Check if arrays are of different length
        if len(y_pred) != len(y_true):
            raise ValueError('Length of y_pred and y_true is not matching')

        # Check if arrays are of different length
        if len(y_pred) != len(y_prob):
            raise ValueError('Length of y_pred and y_prob is not matching')

        # Accuracy Score
        ac = np.round(accuracy_score(y_true, y_pred), 2)
        print('Accuracy Score:', ac, '\n')

        # Confusion Matrix
        cmtx = pd.DataFrame(
        confusion_matrix(y_true, y_pred, labels=[0, 1]), 
        index=['true:0', 'true:1'], 
        columns=['pred:0', 'pred:1'])
        # print('Confusion Matrix:')
        # print(cmtx, '\n')

        # FPR, TPR, AUC
        fpr, tpr, thresholds = roc_curve(y_true, y_pred)
        # print('False Positive Rate:', np.round(fpr[1],2))
        # print('True Positive Rate:', np.round(tpr[1],2))
        print('AUC:', np.round(auc(fpr, tpr), 2), '\n')

        # Sensitivity, Specificity
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        # print('Sensitivity:', np.round(tp/(tp+fn),2))
        # print('Specificity:', np.round(tn/(tn+fp),2), '\n')

        # # F1 Score
        # print('F1 Score:', np.round(f1_score(y_true, y_pred), 2), '\n')

        # # KS Statistic
        # print('KS Score:')
        # print(ks_2samp(y_pred, y_true), '\n')
        
        # # Classification Report
        # print('Classification Report:')
        # print(classification_report(y_true, y_pred))

        # # Gain chart
        # print('Gain Chart')
        # skplt.metrics.plot_cumulative_gain(y_true, y_prob, figsize=(10, 6), title_fontsize=18, text_fontsize=16)
        # plt.show()

        # # Lift chart
        # print('Lift Chart')
        # skplt.metrics.plot_lift_curve(y_true, y_prob, figsize=(10, 6), title_fontsize=18, text_fontsize=16)
        # plt.show()


        output_dict = {'df_tag':tag,'accuracy_score': ac, 'FPR':np.round(fpr[1],2) , 'TPR':np.round(tpr[1],2), 'AUC': np.round(auc(fpr, tpr), 2), 'Sensitivity':np.round(tp/(tp+fn),2), 'Specificity':np.round(tn/(tn+fp),2), 'KS Stat':ks_2samp(y_pred, y_true)}

        return(output_dict)


    # Cross Validation --------------------------------------------------------------------------------------------------------
    def cross_validation(self, model: object, X: np.ndarray, y: np.ndarray, scoring='accuracy', folds=5, seed=42):
        '''
        Returns the cross validation scores for a binary classification model

            Parameters:
                    model (object): a binary classifier model object
                    X (array(int)): multi-dimensional array of predictor variables
                    y (array(int)): array of actual labels consisting of only 0 and 1
                    scoring (str, optional): scoring method to be used, use sklearn.metrics.get_scorer_names() to see the complete list
                    folds (int, optional): total splits of the data to get the scores
            
            Returns:
                    scores (int): cross validated scores of the binary classifer
                    
            Raises:
                    ValueError: If length of X is not equal to length of y
        '''
        # Check if arrays are of different length
        if len(X) != len(y):
            raise ValueError('Length of predictor and dependent is not matching')

        # Define splits
        cv = KFold(n_splits=folds, random_state=seed, shuffle=True)

        # Calculate scores
        scores = cross_val_score(model, X, y, cv=cv, scoring=scoring)
        
        # Return scores
        return scores


    # Feature importance --------------------------------------------------------------------------------------------------------
    def feature_importance(self, model: object, X: pd.DataFrame, imp_type='gain'):
        '''
        Returns the best binary classifier with tuned hyperparameter set

            Parameters:
                    model (object): model object of the binary classifier
                    X (dataframe(pandas)): pandas dataframe of predictor variables in train dataset
                    imp_type (str): importance type to be plotted from the model, choose from ['gain', 'cover', 'weight', 
                                                                                            'total_gain', 'total_cover']
            
            Prints:
                    feat_importances (plot): plots the feature importance

            Returns:
                    feat_importance (dataframe(pandas)): pandas dataframe of feature importances

            Raises:
                    ValueError: If X is not pandas dataframe
        '''
        # Check if train dataset is passed as dataframe
        if type(X) != pd.core.frame.DataFrame:
            raise ValueError('Train dataset not passed as pandas dataframe')
        
        # Check if the classifier is tree based
        if type(model).__name__ in (['DecisionTreeClassifier', 'RandomForestClassifier', 'XGBClassifier']):
            importance_array = np.fromiter(model.get_booster().get_score(importance_type=imp_type).values(), dtype=float)
        else:
            importance_array = model.coef_[0]
        
        # Extract the feature importance
        feat_importances = pd.DataFrame(importance_array, index=X.columns, columns=['importance'])

        # Plot the feature importance
        fig = px.bar(feat_importances, orientation='h')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        fig.show()

        # Return the feature importance dataframe
        return feat_importances


    # Probability binning --------------------------------------------------------------------------------------------------------
    def probability_binned(self,model: object, X: pd.DataFrame, 
                     target_col: str, bins=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1], aggregate_func='median'):
        # Check if train dataset is passed as dataframe
        if type(X) != pd.core.frame.DataFrame:
            raise ValueError('Dataset not passed as pandas dataframe')

        # Get the class probability         
        predicted_probas = model.predict_proba(X.drop(columns=[target_col]))
        X['proba'] = predicted_probas[:,1:].flatten()

        # Define probability bins
        X['binned'] = pd.cut(X['proba'], bins)
        X['volume_binned'] = pd.qcut(X['proba'],q=5)

        # Apply the aggregate function on the target column
        if aggregate_func == 'median':
            prob_dist = X.groupby('binned', as_index=False)[target_col].median()
        elif aggregate_func == 'mean':
            prob_dist = X.groupby('binned', as_index=False)[target_col].mean()
        elif aggregate_func == 'rate':
            prob_dist = X.groupby('volume_binned', as_index=False).apply(lambda x: np.round(
                (x[target_col].sum()/len(x))*100, 2)).rename(columns={None:target_col+'_rate (%)'})
        
        # Add volume info to the aggregate dataframe
        # prob_dist['target'] = X.groupby('binned', as_index=False)[target_col].sum()[target_col]
        prob_dist['volume'] = X.groupby('volume_binned', as_index=False)[target_col].count()[target_col]
        # prob_dist['target_rate'] = np.round((prob_dist['target']/prob_dist['volume'])*100, 2)
        prob_dist['volume %'] = np.round((prob_dist['volume'] / prob_dist['volume'].sum())*100, 2)

        # Return the aggregated dataframe
        return prob_dist

    def rank_ordering_log_reg(self, model, x_train, y_train, x_test, y_test, feature_list):
        df_test = pd.concat([x_test,y_test],axis=1)
        df_train = pd.concat([x_train,y_train],axis=1)

        print(df_test.columns[-1])

        df_all = df_train.append(df_test)
        
        rnk_df_all = self.probability_binned(model,df_all[np.append([feature_list], ['DPD_plus_15'])], 'DPD_plus_15', aggregate_func='rate')
        rnk_df_test = self.probability_binned(model,df_test[np.append([feature_list], ['DPD_plus_15'])], 'DPD_plus_15', aggregate_func='rate')
        rnk_df_train = self.probability_binned(model,df_train[np.append([feature_list], ['DPD_plus_15'])], 'DPD_plus_15', aggregate_func='rate')
        
        return rnk_df_test, rnk_df_train, rnk_df_all


