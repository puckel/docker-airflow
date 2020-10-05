"""
Requires Pandas, Numpy
Example:
In: data = {"primary_key": ["a", "a", "a", "b", "b", "b", "b", "c", "c"],
        "target": ["x", "y", "z", "w", "x", "y", "z", "t", "u"]}
In: example_df = pd.DataFrame.from_dict(data)
Out: 
primary_key	target
0	a	x
1	a	y
2	a	z
3	b	w
4	b	x
5	b	y
6	b	z
7	c	t
8	c	u
In: categorical_encoder(example_df, group_col="primary_key", target_col="target")
Out:
target_t	target_u	target_w	target_x	target_y	target_z
a	0	0	0	1	1	1
b	0	0	1	1	1	1
c	1	1	0	0	0	0
"""

from numpy import squeeze
from numpy import asarray
from numpy import matrix
from pandas import get_dummies
from pandas import DataFrame


def categorical_encoder(dataframe, group_col, target_col):
    """
    dataframe: Pandas dataframe containing data to encode
    group_col: column in dataframe to group into single value
    target_col: column with categorical data that will be encoded into a single vector
    """
    # get one-hot encoded vector for each row
    dummy_df = get_dummies(dataframe, columns=[target_col])

    aggregated_vecs = {}
    # group by group_col, and aggregate each group by summing one-hot encoded vectors
    for name, group in dummy_df.groupby(group_col):
        # get all columns that match our dummy variable prefix
        g = group[group.columns[(group.columns).str.startswith(target_col)]]
        # sum columns together
        aggregated_vec = matrix(g).sum(axis=0)
        #print(np.squeeze(np.asarray(phecode_vec)))
        # turn matrix into vector
        aggregated_vecs[name] = squeeze(asarray(aggregated_vec))
    # create dataframe with dictionary mapping group_col values to aggregated vectors
    aggregated_vecs_df = DataFrame.from_dict(aggregated_vecs, orient="index")
    # add back column names
    aggregated_vecs_df.columns = dummy_df.columns.values[dummy_df.columns.str.startswith(target_col)]
    return aggregated_vecs_df
