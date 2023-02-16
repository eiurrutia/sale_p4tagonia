import numpy as np
import pandas as pd
import pandasql as ps
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder
from sklearn.compose import make_column_transformer
import scipy
from scipy import stats
from scipy.special import boxcox1p
from scipy.stats import norm
from sales.data_holder import DataHolder
from sklearn.pipeline import Pipeline

NUMERICAL_COLS = ['price', 'size_house', 'size_lot', 'size_basement',
                  'latitude', 'longitude', 'avg_size_neighbor_houses', 'avg_size_neighbor_lot']
COLS = ['num_bath', 'num_bed', 'num_floors', 'is_waterfront', 'condition', 'year_built', 'renovation_date', 'zip', 'size_house', 'size_lot', 'size_basement',
        'latitude', 'longitude', 'avg_size_neighbor_houses', 'avg_size_neighbor_lot']

data_holder = DataHolder()
