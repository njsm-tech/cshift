from typing import List

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

from .comparison import Comparison
from ..dataset import Dataset

class LRComparison(Comparison):
    LR_TRAIN_ACC = 'lr_train_acc'
    LR_TEST_ACC = 'lr_test_acc'
    LR_WEIGHT = 'lr_weight'
    LR_TEST_ACC_THRESH = .05  # offset from .5

    @classmethod
    def compare(cls, *datasets):
        cls.validate_datasets(*datasets)
        [ds1, ds2] = datasets
        df1, df2 = ds1.df, ds2.df
        lr_feat1, lr_feat2 = df1.values, df2.values

        # ensure equal sizes 
        if lr_feat1.shape[0] > lr_feat2.shape[0]:
            idx = np.arange(lr_feat1.shape[0])
            idx = np.random.choice(idx, size=lr_feat2.shape[0], replace=False)
            lr_feat1 = lr_feat1[idx]
        else:
            idx = np.arange(lr_feat2.shape[0])
            idx = np.random.choice(idx, size=lr_feat1.shape[0], replace=False)
            lr_feat2 = lr_feat2[idx]

        # create lr features and labels
        lr_feat = np.vstack([lr_feat1, lr_feat2])
        zeros = np.zeros(lr_feat1.shape[0]).reshape(-1, 1)
        ones = np.ones(lr_feat2.shape[0]).reshape(-1, 1)
        lr_labels = np.vstack([zeros, ones])
        lr_X_train, lr_X_test, lr_y_train, lr_y_test = train_test_split(
                lr_feat, lr_labels, test_size=.5)

        # fit model and get performance metrics and weights
        lr_model = LogisticRegression(fit_intercept=False)
        lr_model.fit(lr_X_train, lr_y_train)
        lr_train_acc = lr_model.score(lr_X_train, lr_y_train)
        lr_test_acc = lr_model.score(lr_X_test, lr_y_test)
        lr_weights = lr_model.coef_.flatten()

        # create result dataframe 
        res = {}
        for i, col in enumerate(df1.columns):
            res[col] = {
                cls.LR_TRAIN_ACC: lr_train_acc,
                cls.LR_TEST_ACC: lr_test_acc,
                cls.LR_WEIGHT: lr_weights[i]
                }
        
        res_df = pd.DataFrame.from_dict(res, orient='columns')
        return res_df

    @classmethod
    def shift_detected(cls, *datasets):
        res = cls.compare(*datasets)
        test_accs = res.loc[cls.LR_TEST_ACC].values
        discrim = np.abs(0.5 - test_accs)
        return np.any(discrim > cls.LR_TEST_ACC_THRESH)
